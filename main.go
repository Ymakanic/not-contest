package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Response structures
type (
	CheckoutResponse struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}

	PurchaseResponse struct {
		Message string `json:"message"`
		User string `json:"user"`
		Item string `json:"item"`
	}

	StatusResponse struct {
		SecondsRemaining    int    `json:"seconds_remaining"`
		SuccessfulCheckouts uint64 `json:"successful_checkouts"`
		FailedCheckouts     uint64 `json:"failed_checkouts"`
		SuccessfulPurchases uint64 `json:"successful_purchases"`
		FailedPurchases     uint64 `json:"failed_purchases"`
		ScheduledGoods      uint64 `json:"scheduled_goods"`
		PurchasedGoods      uint64 `json:"purchased_goods"`
		SaleStatus          string `json:"sale_status"`
	}

	ErrorResponse struct {
		Error string `json:"error"`
	}
)

// Global counters
var (
	successfulCheckouts  uint64
	failedCheckouts      uint64
	successfulPurchases  uint64
	failedPurchases      uint64
	scheduledGoods       uint64
	purchasedGoods       uint64
	currentSaleConfirmed uint32 // Atomic flag for sale status
)

var (
	dbPool              *pgxpool.Pool
	redisClient         *redis.Client
	port                string
	certFile            string
	keyFile             string
	reservationTimeout  int
)

func respondWithError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func respondWithJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func recoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Handler panic: %v", err)
				atomic.AddUint64(&failedCheckouts, 1)
				respondWithError(w, http.StatusInternalServerError, "Internal server error")
			}
		}()
		next(w, r)
	}
}

func requestThrottlingMiddleware(limit int, burst int) func(http.Handler) http.Handler {
	tokenBucket := make(chan struct{}, burst)
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(limit))
		defer ticker.Stop()
		for range ticker.C {
			select {
			case tokenBucket <- struct{}{}:
			default:
			}
		}
	}()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-tokenBucket:
				next.ServeHTTP(w, r)
			default:
				respondWithError(w, http.StatusTooManyRequests, "Too Many Requests")
			}
		})
	}
}

func generateUniqueCode() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func createReservation(ctx context.Context, userID, itemID string) (string, error) {
	// Check if current sale is already complete
	if atomic.LoadUint32(&currentSaleConfirmed) == 1 {
		return "", fmt.Errorf("sale completed, items sold out")
	}

	now := float64(time.Now().Unix())
	expireAt := now + float64(reservationTimeout)

	globalKey := "reservations:global"
	userKey := fmt.Sprintf("reservations:user:%s", userID)
	itemKey := fmt.Sprintf("item_reservation:%s", itemID)

	// Check if item is already reserved
	if exists, err := redisClient.Exists(ctx, itemKey).Result(); err != nil {
		return "", err
	} else if exists == 1 {
		return "", fmt.Errorf("item already reserved")
	}

	// Cleanup expired reservations
	if err := redisClient.ZRemRangeByScore(ctx, globalKey, "-inf", fmt.Sprintf("%f", now)).Err(); err != nil {
		return "", err
	}
	if err := redisClient.ZRemRangeByScore(ctx, userKey, "-inf", fmt.Sprintf("%f", now)).Err(); err != nil {
		return "", err
	}

	// Check global and user limits
	globalCount, err := redisClient.ZCard(ctx, globalKey).Result()
	if err != nil {
		return "", err
	}
	if globalCount >= 10000 {
		return "", fmt.Errorf("sale completed, items sold out")
	}

	userCount, err := redisClient.ZCard(ctx, userKey).Result()
	if err != nil {
		return "", err
	}
	if userCount >= 10 {
		return "", fmt.Errorf("purchase limit exceeded for this user")
	}

	// Generate unique reservation code
	code, err := generateUniqueCode()
	if err != nil {
		return "", err
	}

	// Atomically reserve the item
	txf := func(tx *redis.Tx) error {
		// Check if item was reserved while we were processing
		if exists, err := tx.Exists(ctx, itemKey).Result(); err != nil {
			return err
		} else if exists == 1 {
			return fmt.Errorf("item reservation conflict")
		}

		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SetEX(ctx, itemKey, code, time.Duration(reservationTimeout)*time.Second)
			pipe.ZAdd(ctx, globalKey, &redis.Z{Score: expireAt, Member: code})
			pipe.ZAdd(ctx, userKey, &redis.Z{Score: expireAt, Member: code})
			pipe.Set(ctx, fmt.Sprintf("reservation:%s", code), 
				fmt.Sprintf("%s|%s", userID, itemID), 
				time.Duration(reservationTimeout)*time.Second)
			return nil
		})
		return err
	}

	// Retry transaction if key changed
	for i := 0; i < 3; i++ {
		err := redisClient.Watch(ctx, txf, itemKey)
		if err == nil {
			// Success
			return code, nil
		}
		if err == redis.TxFailedErr {
			// Conflict, retry
			continue
		}
		return "", err
	}

	return "", fmt.Errorf("item reservation failed after retries")
}

func checkout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	userID := r.URL.Query().Get("user_id")
	itemID := r.URL.Query().Get("id")
	if userID == "" || itemID == "" {
		atomic.AddUint64(&failedCheckouts, 1)
		respondWithError(w, http.StatusBadRequest, "Missing user_id or id parameters")
		return
	}

	ctx := r.Context()
	code, err := createReservation(ctx, userID, itemID)
	if err != nil {
		log.Printf("Reservation error: %v", err)
		atomic.AddUint64(&failedCheckouts, 1)
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	sqlInsert := `INSERT INTO checkout_attempts (user_id, item_id, code) VALUES ($1, $2, $3)`
	if _, err := dbPool.Exec(ctx, sqlInsert, userID, itemID, code); err != nil {
		log.Printf("Error saving checkout_attempt: %v", err)
		atomic.AddUint64(&failedCheckouts, 1)
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	atomic.AddUint64(&successfulCheckouts, 1)
	atomic.AddUint64(&scheduledGoods, 1)
	respondWithJSON(w, http.StatusOK, CheckoutResponse{
		Message: "success",
		Code:    code,
	})
}

func purchase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusBadRequest, "Missing code parameter")
		return
	}

	ctx := r.Context()
	reservationKey := fmt.Sprintf("reservation:%s", code)
	val, err := redisClient.Get(ctx, reservationKey).Result()
	if err == redis.Nil {
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusBadRequest, "Reservation not found or expired")
		return
	} else if err != nil {
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusInternalServerError, "Redis error")
		return
	}

	parts := strings.Split(val, "|")
	if len(parts) != 2 {
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusInternalServerError, "Invalid reservation data format")
		return
	}

	userID := parts[0]
	itemID := parts[1]

	// Release item reservation
	itemKey := fmt.Sprintf("item_reservation:%s", itemID)
	redisClient.Del(ctx, itemKey)

	// Cleanup reservation data
	globalKey := "reservations:global"
	userKey := fmt.Sprintf("reservations:user:%s", userID)
	redisClient.ZRem(ctx, globalKey, code)
	redisClient.ZRem(ctx, userKey, code)
	redisClient.Del(ctx, reservationKey)

	// Process purchase in database
	tx, err := dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Transaction begin error: %v", err)
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}
	defer tx.Rollback(ctx)

	sqlInsertSale := `INSERT INTO sales (user_id, item_id, status) VALUES ($1, $2, 'pending')`
	if _, err := tx.Exec(ctx, sqlInsertSale, userID, itemID); err != nil {
		log.Printf("Sales insert error: %v", err)
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	sqlUpdateCheckout := `UPDATE checkout_attempts SET used = true WHERE code = $1`
	if _, err := tx.Exec(ctx, sqlUpdateCheckout, code); err != nil {
		log.Printf("Checkout update error: %v", err)
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Transaction commit error: %v", err)
		atomic.AddUint64(&failedPurchases, 1)
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	atomic.AddUint64(&successfulPurchases, 1)
	atomic.AddUint64(&purchasedGoods, 1)
	respondWithJSON(w, http.StatusOK, PurchaseResponse{
		Message: "success",
		User: userID,
		Item: itemID,
	})
}

func finalizeSales(ctx context.Context) error {
	tx, err := dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction begin error: %w", err)
	}
	defer tx.Rollback(ctx)
	
	// Get the previous hour's boundaries
	now := time.Now()
	prevHour := now.Truncate(time.Hour).Add(-time.Hour)
	prevHourEnd := prevHour.Add(time.Hour)
	
	var pendingCount int
	sqlCount := `SELECT COUNT(*) FROM sales 
		WHERE status = 'pending' 
		AND purchased_at >= $1 
		AND purchased_at < $2`
	
	if err := tx.QueryRow(ctx, sqlCount, prevHour, prevHourEnd).Scan(&pendingCount); err != nil {
		return fmt.Errorf("pending sales count error: %w", err)
	}
	
	if pendingCount == 10000 {
		sqlConfirm := `UPDATE sales 
			SET status = 'confirmed', committed_at = now() 
			WHERE status = 'pending' 
			AND purchased_at >= $1 
			AND purchased_at < $2`
		if _, err := tx.Exec(ctx, sqlConfirm, prevHour, prevHourEnd); err != nil {
			return fmt.Errorf("sales confirmation error: %w", err)
		}
		log.Println("Sales confirmed - exactly 10000 orders")
		atomic.StoreUint32(&currentSaleConfirmed, 1)
	} else {
		sqlDelete := `DELETE FROM sales 
			WHERE status = 'pending' 
			AND purchased_at >= $1 
			AND purchased_at < $2`
		if _, err := tx.Exec(ctx, sqlDelete, prevHour, prevHourEnd); err != nil {
			return fmt.Errorf("pending sales deletion error: %w", err)
		}
		log.Printf("Provisional sales count (%d) not equal to 10000. Sales canceled\n", pendingCount)
	}
	
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("transaction commit error: %w", err)
	}
	
	// Reset metrics for new sale
	atomic.StoreUint64(&successfulCheckouts, 0)
	atomic.StoreUint64(&failedCheckouts, 0)
	atomic.StoreUint64(&successfulPurchases, 0)
	atomic.StoreUint64(&failedPurchases, 0)
	atomic.StoreUint64(&scheduledGoods, 0)
	atomic.StoreUint64(&purchasedGoods, 0)
	atomic.StoreUint32(&currentSaleConfirmed, 0)
	
	// Clear Redis for new sale
	if err := resetRedis(ctx); err != nil {
		log.Printf("Redis reset error: %v", err)
	}
	
	return nil
}

func resetRedis(ctx context.Context) error {
	// Clear global reservations
	globalKey := "reservations:global"
	if err := redisClient.Del(ctx, globalKey).Err(); err != nil {
		return err
	}
	
	// Clear user reservations
	iter := redisClient.Scan(ctx, 0, "reservations:user:*", 0).Iterator()
	for iter.Next(ctx) {
		if err := redisClient.Del(ctx, iter.Val()).Err(); err != nil {
			log.Printf("Error deleting key %s: %v", iter.Val(), err)
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	
	// Clear reservation details
	iter = redisClient.Scan(ctx, 0, "reservation:*", 0).Iterator()
	for iter.Next(ctx) {
		if err := redisClient.Del(ctx, iter.Val()).Err(); err != nil {
			log.Printf("Error deleting key %s: %v", iter.Val(), err)
		}
	}
	
	// Clear item reservations
	iter = redisClient.Scan(ctx, 0, "item_reservation:*", 0).Iterator()
	for iter.Next(ctx) {
		if err := redisClient.Del(ctx, iter.Val()).Err(); err != nil {
			log.Printf("Error deleting key %s: %v", iter.Val(), err)
		}
	}
	return iter.Err()
}

func status(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	nextHour := now.Truncate(time.Hour).Add(time.Hour)
	secondsRemaining := int(nextHour.Sub(now).Seconds())
	
	respondWithJSON(w, http.StatusOK, StatusResponse{
		SecondsRemaining:    secondsRemaining,
		SuccessfulCheckouts: atomic.LoadUint64(&successfulCheckouts),
		FailedCheckouts:     atomic.LoadUint64(&failedCheckouts),
		SuccessfulPurchases: atomic.LoadUint64(&successfulPurchases),
		FailedPurchases:     atomic.LoadUint64(&failedPurchases),
		ScheduledGoods:      atomic.LoadUint64(&scheduledGoods),
		PurchasedGoods:      atomic.LoadUint64(&purchasedGoods),
		SaleStatus:          saleStatusText(),
	})
}

func saleStatusText() string {
	if atomic.LoadUint32(&currentSaleConfirmed) == 1 {
		return "completed"
	}
	return "active"
}

func scheduleFinalization() {
	for {
		now := time.Now()
		nextHour := now.Truncate(time.Hour).Add(time.Hour)
		waitDuration := nextHour.Sub(now)
		log.Printf("Waiting %v for sales finalization...", waitDuration)
		time.Sleep(waitDuration)
		ctx := context.Background()
		if err := finalizeSales(ctx); err != nil {
			log.Printf("Sales finalization error: %v", err)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func initDB() error {
	ctx := context.Background()
	queries := []string{
		`CREATE TABLE IF NOT EXISTS checkout_attempts (
			id SERIAL PRIMARY KEY,
			user_id TEXT NOT NULL,
			item_id TEXT NOT NULL,
			code TEXT NOT NULL UNIQUE,
			created_at TIMESTAMP DEFAULT NOW(),
			used BOOLEAN DEFAULT FALSE
		)`,
		`CREATE TABLE IF NOT EXISTS sales (
			id SERIAL PRIMARY KEY,
			user_id TEXT NOT NULL,
			item_id TEXT NOT NULL,
			status VARCHAR(20) NOT NULL,
			purchased_at TIMESTAMP DEFAULT NOW(),
			committed_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS sales_status_idx ON sales(status)`,
		`CREATE INDEX IF NOT EXISTS sales_purchased_idx ON sales(purchased_at)`,
	}

	for _, q := range queries {
		if _, err := dbPool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	// Environment variables with defaults
	port = getEnv("PORT", "8080")
	certFile = os.Getenv("CERT_FILE")
	keyFile = os.Getenv("KEY_FILE")
	reservationTimeout, _ = strconv.Atoi(getEnv("RESERVATION_TIMEOUT", "600"))

	// PostgreSQL setup
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		getEnv("PG_USER", "postgres"),
		getEnv("PG_PASSWORD", "postgres"),
		getEnv("PG_HOST", "postgres"),
		getEnv("PG_PORT", "5432"),
		getEnv("PG_DB", "sales"),
	)
	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer dbPool.Close()

	// Redis setup
	redisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", getEnv("REDIS_HOST", "redis"), getEnv("REDIS_PORT", "6379")),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       0,
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis connection error: %v", err)
	}

	// Database initialization
	if err := initDB(); err != nil {
		log.Fatalf("Database init error: %v", err)
	}

	// Initialize sale status
	atomic.StoreUint32(&currentSaleConfirmed, 0)

	go scheduleFinalization()

	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", recoverMiddleware(checkout))
	mux.HandleFunc("/purchase", recoverMiddleware(purchase))
	mux.HandleFunc("/status", recoverMiddleware(status))

	handlerWithThrottle := requestThrottlingMiddleware(10, 20)(mux)
	addr := fmt.Sprintf(":%s", port)
	server := &http.Server{
		Addr:         addr,
		Handler:      handlerWithThrottle,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-quit
		log.Printf("Received signal %v. Starting graceful shutdown...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Fatalf("Server shutdown error: %v", err)
		}
		log.Println("Server stopped gracefully")
	}()

	if certFile != "" && keyFile != "" {
		log.Printf("Starting HTTPS server on :%s", port)
		if err := server.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTPS server error: %v", err)
		}
	} else {
		log.Printf("Starting HTTP server on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}
}

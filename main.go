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
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	dbPool            *pgxpool.Pool
	redisClient       *redis.Client
	port              string
	certFile          string
	keyFile           string
	reservationTimeout int
)

func recoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Handler panic: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
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
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
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
	now := float64(time.Now().Unix())
	expireAt := now + float64(reservationTimeout)

	globalKey := "reservations:global"
	userKey := fmt.Sprintf("reservations:user:%s", userID)

	if err := redisClient.ZRemRangeByScore(ctx, globalKey, "-inf", fmt.Sprintf("%f", now)).Err(); err != nil {
		return "", err
	}
	if err := redisClient.ZRemRangeByScore(ctx, userKey, "-inf", fmt.Sprintf("%f", now)).Err(); err != nil {
		return "", err
	}

	globalCount, err := redisClient.ZCard(ctx, globalKey).Result()
	if err != nil {
		return "", err
	}
	if globalCount >= 10000 {
		return "", fmt.Errorf("Sale completed, items sold out")
	}

	userCount, err := redisClient.ZCard(ctx, userKey).Result()
	if err != nil {
		return "", err
	}
	if userCount >= 10 {
		return "", fmt.Errorf("Purchase limit exceeded for this user")
	}

	code, err := generateUniqueCode()
	if err != nil {
		return "", err
	}

	if err := redisClient.ZAdd(ctx, globalKey, &redis.Z{Score: expireAt, Member: code}).Err(); err != nil {
		return "", err
	}
	if err := redisClient.ZAdd(ctx, userKey, &redis.Z{Score: expireAt, Member: code}).Err(); err != nil {
		redisClient.ZRem(ctx, globalKey, code)
		return "", err
	}

	reservationKey := fmt.Sprintf("reservation:%s", code)
	val := fmt.Sprintf("%s|%s", userID, itemID)
	if err := redisClient.Set(ctx, reservationKey, val, time.Duration(reservationTimeout)*time.Second).Err(); err != nil {
		redisClient.ZRem(ctx, globalKey, code)
		redisClient.ZRem(ctx, userKey, code)
		return "", err
	}
	return code, nil
}

func checkout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	itemID := r.URL.Query().Get("id")
	if userID == "" || itemID == "" {
		http.Error(w, "Missing user_id or id parameters", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	code, err := createReservation(ctx, userID, itemID)
	if err != nil {
		log.Printf("Reservation error: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sqlInsert := `INSERT INTO checkout_attempts (user_id, item_id, code) VALUES ($1, $2, $3)`
	if _, err := dbPool.Exec(ctx, sqlInsert, userID, itemID, code); err != nil {
		log.Printf("Error saving checkout_attempt: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Your reservation code: %s\n", code)
}

func purchase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code parameter", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	reservationKey := fmt.Sprintf("reservation:%s", code)
	val, err := redisClient.Get(ctx, reservationKey).Result()
	if err == redis.Nil {
		http.Error(w, "Reservation not found or expired", http.StatusBadRequest)
		return
	} else if err != nil {
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}
	parts := strings.Split(val, "|")
	if len(parts) != 2 {
		http.Error(w, "Invalid reservation data format", http.StatusInternalServerError)
		return
	}
	userID := parts[0]
	itemID := parts[1]

	globalKey := "reservations:global"
	userKey := fmt.Sprintf("reservations:user:%s", userID)
	redisClient.ZRem(ctx, globalKey, code)
	redisClient.ZRem(ctx, userKey, code)
	redisClient.Del(ctx, reservationKey)

	tx, err := dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Transaction begin error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)
	sqlInsertSale := `INSERT INTO sales (user_id, item_id, status) VALUES ($1, $2, 'pending')`
	if _, err := tx.Exec(ctx, sqlInsertSale, userID, itemID); err != nil {
		log.Printf("Sales insert error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	sqlUpdateCheckout := `UPDATE checkout_attempts SET used = true WHERE code = $1`
	if _, err := tx.Exec(ctx, sqlUpdateCheckout, code); err != nil {
		log.Printf("Checkout update error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Transaction commit error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Provisional purchase confirmed for user %s, item %s\n", userID, itemID)
}

func finalizeSales(ctx context.Context) error {
	tx, err := dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Transaction begin error: %w", err)
	}
	defer tx.Rollback(ctx)
	var pendingCount int
	sqlCount := `SELECT COUNT(*) FROM sales WHERE status = 'pending' AND purchased_at >= date_trunc('hour', now())`
	if err := tx.QueryRow(ctx, sqlCount).Scan(&pendingCount); err != nil {
		return fmt.Errorf("Pending sales count error: %w", err)
	}
	if pendingCount == 10000 {
		sqlConfirm := `UPDATE sales SET status = 'confirmed', committed_at = now() WHERE status = 'pending' AND purchased_at >= date_trunc('hour', now())`
		if _, err := tx.Exec(ctx, sqlConfirm); err != nil {
			return fmt.Errorf("Sales confirmation error: %w", err)
		}
		log.Println("Sales confirmed - exactly 10000 orders")
	} else {
		sqlDelete := `DELETE FROM sales WHERE status = 'pending' AND purchased_at >= date_trunc('hour', now())`
		if _, err := tx.Exec(ctx, sqlDelete); err != nil {
			return fmt.Errorf("Pending sales deletion error: %w", err)
		}
		log.Printf("Provisional sales count (%d) not equal to 10000. Sales canceled\n", pendingCount)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("Transaction commit error: %w", err)
	}
	return nil
}

func timerHandler(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	nextHour := now.Truncate(time.Hour).Add(time.Hour)
	secondsRemaining := int(nextHour.Sub(now).Seconds())
	resp := map[string]interface{}{
		"seconds_remaining": secondsRemaining,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
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

	go scheduleFinalization()

	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", recoverMiddleware(checkout))
	mux.HandleFunc("/purchase", recoverMiddleware(purchase))
	mux.HandleFunc("/timer", recoverMiddleware(timerHandler))
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

# Flash Sale Service
## Build
First clone this repository to your machine:\
`git clone https://github.com/Ymakanic/not-contest.git`

Then open repository folder:\
`cd not-contest && ls`\
We can see main files, files used for building and .env file which contains app configuration. You can leave it as is, it will work the same. If not you can change ports, db users etc.

Then we'll create separated terminal instance to run container in background:\
`screen -S flashsale`\
Or other method if you prefer.\
When you see clear terminal we're ready to build. We'll make it using following command:\
`make init`\
Now application will built, including databases and tables, you only need enjoy watching it). Isn't it awesome?\
Once app is build you can detach separated terminal using `Ctrl + A + D`

### Now our app is listening port 8080 (if you did not change that) and accepts 3 methods:
- GET /status
- POST /checkout?user_id=%user%&id=%item%
- POST /purchase?code=%code%

### Response examlpes for /status:
Success:\
`{"seconds_remaining":695,"successful_checkouts":10,"failed_checkouts":6,"successful_purchases":1,"failed_purchases":0,"scheduled_goods":10,"purchased_goods":1,"sale_status":"active"}`\
Displays current service status

### Response examlpes for /checkout:
Success:\
`{"message":"success","code":"a320c2f45edcc6ebd16041e051ebb0c9"}`\
Displays scheduled item code.

Trying to reserve already scheduled item:\
`{"error":"item already reserved"}`

Trying to reserve more than 10 items:\
`{"error":"purchase limit exceeded for this user"}`

### Response examples for /purchase:
Success:\
`{"message":"success","user":"1","item":"10"}`

Trying purchase twice or with non existing code:\
`{"error":"Reservation not found or expired"}`

## Explanation
We save info about checkouts in Postgre database, ensuring we won't sell more than 10 items to user and won't sell an item twice. If user tries to purchase more than 10000 items, then purchase won't be completed. If we sell less than 10000 items in hour, all purchases will be canceled.\
Status and counter will be reset every hour.

## Testing
You can check endpoints using curl. There are some examples:

/checkout:\
`curl -X POST "http://127.0.0.1:8080/checkout?user_id=1&id=1"`

/purchase:\
`curl -X POST "http://127.0.0.1:8080/purchase?code=493759887b899806c494e780e86ec503"`

/status:\
`curl -X GET "http://127.0.0.1:8080/status"`

ACCOUNT_SCHEMA = {
    "account_id": "string",
    "account_type": "string",
    "balance": "double",
    "opened_date": "date",
    "status": "string"
}

CUSTOMER_SCHEMA = {
    "customer_id": "string",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "phone": "string",
    "address": "string",
    "created_date": "date"
}

TRANSACTION_SCHEMA = {
    "transaction_id": "string",
    "account_id": "string",
    "transaction_type": "string",
    "amount": "double",
    "transaction_date": "timestamp"
}
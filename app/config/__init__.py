
import os

DEMO_DB_CONFIG = {
    "database": "postgres",
    "user": "datavio",
    "host": "datavio-dev-db.postgres.database.azure.com",
    "password": os.environ.get("DEMODB_PWD"),
    "port": "5432"
}

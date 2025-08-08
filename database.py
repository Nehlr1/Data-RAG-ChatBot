import os
import psycopg2
from psycopg2 import sql


class DatabaseManager:
    """Encapsulates database setup and migration utilities."""

    def __init__(self, db_config: dict) -> None:
        self.db_config = db_config

    def create_database_if_not_exists(self) -> None:
        """Create the target database if it doesn't already exist."""
        try:
            target_db_name = self.db_config.get("dbname")
            if not target_db_name:
                raise ValueError("DB_NAME is not set in environment variables")

            # Connect to an admin DB to manage databases (can't connect to a DB that doesn't exist)
            admin_config = self.db_config.copy()
            admin_config["dbname"] = os.getenv("DB_USER", "postgres")

            conn = psycopg2.connect(**admin_config)
            conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction
            cursor = conn.cursor()

            # Check if target database already exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db_name,))
            exists = cursor.fetchone() is not None

            if not exists:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {} ").format(sql.Identifier(target_db_name))
                )
                print("✅ Database created successfully!")
            else:
                print("✅ Database already exists.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error creating database: {e}")
            print("💡 Ensure PostgreSQL is running and that your role has CREATEDB privilege. You can set admin DB via DB_ADMIN_DB (default: 'postgres').")
            exit(1)

    def create_chat_table(self) -> None:
        """Create the chat_history table if it doesn't exist."""
        create_table_sql = """
        -- Create the chat_history table
        CREATE TABLE IF NOT EXISTS chat_history (
            id SERIAL PRIMARY KEY,
            conversation_id VARCHAR(255) UNIQUE NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            messages JSONB NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Create an index on conversation_id for faster lookups
        CREATE INDEX IF NOT EXISTS idx_chat_history_conversation_id ON chat_history(conversation_id);

        -- Create an index on user_id for faster user-based queries
        CREATE INDEX IF NOT EXISTS idx_chat_history_user_id ON chat_history(user_id);

        -- Create an index on created_at for time-based queries 
        CREATE INDEX IF NOT EXISTS idx_chat_history_created_at ON chat_history(created_at);

        -- Add a trigger to automatically update the updated_at timestamp
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';

        DROP TRIGGER IF EXISTS update_chat_history_updated_at ON chat_history;
        CREATE TRIGGER update_chat_history_updated_at 
            BEFORE UPDATE ON chat_history 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Database table verified/created successfully!")
        except Exception as e:
            print(f"❌ Error creating database table: {e}")
            print("💡 Make sure PostgreSQL is running and the database 'chat_history' exists.")
            exit(1)
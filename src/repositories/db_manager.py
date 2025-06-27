import sqlite3

## MODULARIZAR O MANAGER DB PARA 1- CONEXÃO COM O BANCO, 2- CRIAR TABELAS, 3- INSERIR, DELETER OU ATUALIZAR DADOS DO BANCO

class CollectionDB:
    """
    Manages the SQLite database for collection and owner information.
    """
    def __init__(self, db_name='legal_collections.db'):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Establishes a connection to the database."""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            # Enable foreign key constraints
            self.cursor.execute("PRAGMA foreign_keys = ON;")
            print(f"Successfully connected to database: {self.db_name}")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def _create_tables(self):
        """Creates the 'owners' and 'collections' tables if they don't exist."""
        try:
            # Create owners table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS owners (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    cpf TEXT UNIQUE NOT NULL,
                    unit TEXT,
                    block TEXT
                )
            ''')

            # Create collections table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS collections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER NOT NULL,
                    process_number TEXT NOT NULL,
                    last_notification_date TEXT,
                    default_amount REAL NOT NULL,
                    collection_status TEXT NOT NULL,
                    notes TEXT,
                    FOREIGN KEY (owner_id) REFERENCES owners(id)
                )
            ''')
            self.conn.commit()
            print("Tables 'owners' and 'collections' created successfully (if they didn't exist).")
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
            self.conn.rollback()
            raise

    def add_owner(self, name, cpf, unit=None, block=None):
        """
        Adds a new owner to the 'owners' table.
        If an owner with the given CPF already exists, returns their ID.
        Returns the ID of the inserted or existing owner.
        """
        try:
            # Check if owner already exists by CPF
            self.cursor.execute("SELECT id FROM owners WHERE cpf = ?", (cpf,))
            existing_owner = self.cursor.fetchone()

            if existing_owner:
                print(f"Owner with CPF {cpf} already exists. Returning existing ID.")
                return existing_owner[0]
            else:
                self.cursor.execute(
                    "INSERT INTO owners (name, cpf, unit, block) VALUES (?, ?, ?, ?)",
                    (name, cpf, unit, block)
                )
                self.conn.commit()
                owner_id = self.cursor.lastrowid
                print(f"Owner '{name}' added with ID: {owner_id}")
                return owner_id
        except sqlite3.IntegrityError as e:
            print(f"Integrity error adding owner: {e}")
            self.conn.rollback()
            return None
        except sqlite3.Error as e:
            print(f"Error adding owner: {e}")
            self.conn.rollback()
            return None

    def add_collection(self, owner_id, process_number, default_amount, collection_status, last_notification_date=None, notes=None):
        """
        Adds a new collection to the 'collections' table.
        Requires an existing owner_id.
        Returns the ID of the inserted collection.
        """
        try:
            self.cursor.execute(
                """INSERT INTO collections (owner_id, process_number, default_amount,
                                           collection_status, last_notification_date, notes)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (owner_id, process_number, default_amount, collection_status, last_notification_date, notes)
            )
            self.conn.commit()
            collection_id = self.cursor.lastrowid
            print(f"Collection for owner ID {owner_id} (Process: {process_number}) added with ID: {collection_id}")
            return collection_id
        except sqlite3.IntegrityError as e:
            print(f"Integrity error adding collection: {e}. Make sure owner_id exists.")
            self.conn.rollback()
            return None
        except sqlite3.Error as e:
            print(f"Error adding collection: {e}")
            self.conn.rollback()
            return None

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print(f"Database connection to {self.db_name} closed.")

# Example usage:
if __name__ == "__main__":
    db_manager = CollectionDB()

    # --- Add Owners ---
    owner1_id = db_manager.add_owner("João Silva", "111.222.333-44", "101", "A")
    owner2_id = db_manager.add_owner("Maria Oliveira", "555.666.777-88", "203", "B")
    owner3_id = db_manager.add_owner("João Silva", "111.222.333-44", "101", "A") # This will show as existing

    # --- Add Collections ---
    if owner1_id:
        db_manager.add_collection(
            owner_id=owner1_id,
            process_number="PROC-2025-001",
            default_amount=1500.50,
            collection_status="agreement in progress",
            last_notification_date="2025-06-15",
            notes="Initial contact made."
        )
        db_manager.add_collection(
            owner_id=owner1_id,
            process_number="PROC-2025-002",
            default_amount=800.00,
            collection_status="no agreement",
            last_notification_date="2025-05-20",
            notes="Client unresponsive."
        )

    if owner2_id:
        db_manager.add_collection(
            owner_id=owner2_id,
            process_number="PROC-2025-003",
            default_amount=3200.75,
            collection_status="agreement reached",
            last_notification_date="2025-06-01",
            notes="Payment plan agreed."
        )

    db_manager.close()
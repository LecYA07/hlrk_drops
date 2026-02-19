import sqlite3
import argparse
import sys
import yaml

# Load config to get db path
try:
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        db_path = config.get("database", {}).get("db_path", "rewards.db")
except FileNotFoundError:
    db_path = "rewards.db"

def init_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT,
            weight INTEGER,
            quantity INTEGER DEFAULT 1,
            enabled INTEGER DEFAULT 1
        )
    ''')
    
    # Check if 'quantity' column exists (for migration)
    cursor.execute("PRAGMA table_info(rewards)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'quantity' not in columns:
        print("Migrating database: Adding 'quantity' column to 'rewards' table...")
        cursor.execute("ALTER TABLE rewards ADD COLUMN quantity INTEGER DEFAULT 1")
    
    conn.commit()
    conn.close()

def add_reward(name, description, weight, quantity, enabled):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO rewards (name, description, weight, quantity, enabled) VALUES (?, ?, ?, ?, ?)", (name, description, weight, quantity, enabled))
    conn.commit()
    print(f"Reward added: {name} (Weight: {weight}, Quantity: {quantity}, Enabled: {enabled})")
    conn.close()

def list_rewards():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, weight, quantity, enabled, description FROM rewards")
    rows = cursor.fetchall()
    
    # Calculate total weight for chance calculation
    total_weight = sum(row[2] for row in rows if row[4] == 1) # Only enabled rewards count for chance
    
    print(f"{'ID':<5} {'Name':<20} {'Weight':<8} {'Qty':<5} {'Chance':<8} {'Enabled':<8} {'Description'}")
    print("-" * 80)
    for row in rows:
        weight = row[2]
        is_enabled = row[4]
        chance_str = "0%"
        if is_enabled and total_weight > 0:
            chance = (weight / total_weight) * 100
            chance_str = f"{chance:.1f}%"
        elif not is_enabled:
            chance_str = "OFF"
            
        print(f"{row[0]:<5} {row[1]:<20} {row[2]:<8} {row[3]:<5} {chance_str:<8} {is_enabled:<8} {row[5]}")
    conn.close()

def toggle_reward(reward_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT enabled FROM rewards WHERE id = ?", (reward_id,))
    row = cursor.fetchone()
    if row:
        new_status = 0 if row[0] else 1
        cursor.execute("UPDATE rewards SET enabled = ? WHERE id = ?", (new_status, reward_id))
        conn.commit()
        print(f"Reward {reward_id} toggled to enabled={new_status}")
    else:
        print(f"Reward with ID {reward_id} not found.")
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Manage Twitch Rewards")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Add Reward
    parser_add = subparsers.add_parser("add_reward", help="Add a new reward")
    parser_add.add_argument("name", help="Name of the reward")
    parser_add.add_argument("description", help="Description of the reward")
    parser_add.add_argument("weight", type=int, help="Weight for random selection (Higher = more frequent)")
    parser_add.add_argument("--quantity", type=int, default=1, help="Number of winners per drop (Default: 1)")
    parser_add.add_argument("--enabled", type=int, choices=[0, 1], default=1, help="Enabled status (0 or 1)")

    # List Rewards
    parser_list = subparsers.add_parser("list_rewards", help="List all rewards")

    # Toggle Reward
    parser_toggle = subparsers.add_parser("toggle_reward", help="Toggle enabled status of a reward")
    parser_toggle.add_argument("id", type=int, help="ID of the reward to toggle")

    args = parser.parse_args()

    init_db()

    if args.command == "add_reward":
        add_reward(args.name, args.description, args.weight, args.quantity, args.enabled)
    elif args.command == "list_rewards":
        list_rewards()
    elif args.command == "toggle_reward":
        toggle_reward(args.id)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

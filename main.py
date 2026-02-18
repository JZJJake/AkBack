from src.data_loader import DataLoader
import time

def main():
    print("AShare_Sniper Data Sync Started...")
    start_time = time.time()

    loader = DataLoader()

    # Run sync process
    loader.sync_all_data()

    elapsed = time.time() - start_time
    print(f"\nData sync completed in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()

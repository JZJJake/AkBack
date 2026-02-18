import argparse
import os
import sys
import logging
from datetime import datetime
import matplotlib
matplotlib.use('Agg') # Headless mode
import matplotlib.pyplot as plt
import pandas as pd

from src.utils.tdx_runner import TdxServerManager
from src.data_loader import DataLoader
from src.data_enhancer import DataEnhancer
from src.backtest.runner import PortfolioRunner
from src.strategies.sniper_selector import SniperSelector
from src.analysis.metrics import calculate_metrics
from src.config import LOG_DIR

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, "backtest_run.log")),
            logging.StreamHandler()
        ]
    )

def plot_curve(curve_df, filename):
    """Plot total assets curve."""
    plt.figure(figsize=(12, 6))
    plt.plot(curve_df['date'], curve_df['total_assets'], label='Total Assets')
    plt.title('Backtest Equity Curve')
    plt.xlabel('Date')
    plt.ylabel('Assets (RMB)')
    plt.legend()
    plt.grid(True)
    plt.savefig(filename)
    plt.close()

def main():
    parser = argparse.ArgumentParser(description="AShare Sniper Backtest Runner")
    parser.add_argument("--update", action="store_true", help="Run daily data sync (K-line)")
    parser.add_argument("--enhance", action="store_true", help="Run tick data enhancement")
    parser.add_argument("--start-date", type=str, default="2023-01-01", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="Backtest end date")
    parser.add_argument("--capital", type=float, default=100000.0, help="Initial capital")

    args = parser.parse_args()
    setup_logging()

    # 1. Start Server Logic
    # The server is primarily needed for DataEnhancer.
    # However, to be safe and follow architecture instructions, we manage it here if enhancing.
    # Or just always start? "Workflow: Start TDX Server (managed)."
    # But if we only backtest with local files, server is useless overhead.
    # I will start it ONLY if --enhance is True, OR if I need it for some other reason.
    # But wait, DataLoader doesn't need it (uses AkShare).
    # Backtest uses local parquet.
    # So strictly only needed for --enhance.

    server = None
    if args.enhance:
        try:
            logging.info("Starting TDX Server for data enhancement...")
            server = TdxServerManager()
            server.start()
        except Exception as e:
            logging.error(f"Failed to start TDX Server: {e}")
            sys.exit(1)

    try:
        # 2. Update Daily Data
        if args.update:
            logging.info("Starting Daily Data Sync...")
            loader = DataLoader()
            loader.sync_all_data()
            logging.info("Daily Data Sync Complete.")

        # 3. Enhance Tick Data
        if args.enhance:
            logging.info("Starting Tick Data Enhancement...")
            enhancer = DataEnhancer()
            # Default to last 30 days to save time unless full history requested?
            # User requirement: "Iterate through dates (or allow updating a specific date range)."
            # Enhancer.run_all default is 30 days.
            enhancer.run_all(days_limit=30)
            logging.info("Tick Data Enhancement Complete.")

    finally:
        # Stop server if we started it
        if server:
            server.stop()

    # 4. Backtest
    logging.info("Initializing Backtest...")
    loader = DataLoader()

    # Strategy
    try:
        selector = SniperSelector(loader)
    except Exception as e:
        logging.error(f"Failed to initialize Strategy: {e}")
        sys.exit(1)

    # Runner
    runner = PortfolioRunner(loader, initial_capital=args.capital)

    logging.info(f"Running Backtest from {args.start_date} to {args.end_date}...")
    start_time = datetime.now()

    results = runner.run(selector, args.start_date, args.end_date)

    duration = datetime.now() - start_time
    logging.info(f"Backtest complete in {duration}.")

    # 5. Analysis
    trades = results.get("trades", pd.DataFrame())
    curve = results.get("curve", pd.DataFrame())

    if curve.empty:
        logging.warning("No trades or curve generated. Check data availability or strategy logic.")
        return

    metrics = calculate_metrics(curve, trades)

    print("\n" + "="*40)
    print("BACKTEST PERFORMANCE METRICS")
    print("="*40)
    for k, v in metrics.items():
        print(f"{k:<20}: {v:.4f}")
    print("="*40)

    # Plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    plot_path = os.path.join(LOG_DIR, f"backtest_{timestamp}.png")
    plot_curve(curve, plot_path)
    print(f"\nEquity curve saved to: {plot_path}")

    if not trades.empty:
        csv_path = os.path.join(LOG_DIR, f"trades_{timestamp}.csv")
        trades.to_csv(csv_path, index=False)
        print(f"Trade log saved to: {csv_path}")

if __name__ == "__main__":
    main()

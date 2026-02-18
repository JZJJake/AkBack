import pandas as pd
import numpy as np

def calculate_metrics(daily_curve_df, trades_df=None):
    """
    Calculate backtest performance metrics.

    Args:
        daily_curve_df (pd.DataFrame): Columns ['date', 'total_assets', ...]
        trades_df (pd.DataFrame): Columns ['action', 'net_revenue', 'total_cost', ...]

    Returns:
        dict: Performance metrics
    """
    if daily_curve_df is None or daily_curve_df.empty:
        return {}

    # Ensure date is datetime
    df = daily_curve_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values('date', inplace=True)

    # Calculate daily returns
    df['daily_return'] = df['total_assets'].pct_change().fillna(0.0)

    # 1. Total Return
    initial_assets = df['total_assets'].iloc[0]
    final_assets = df['total_assets'].iloc[-1]
    total_return = (final_assets - initial_assets) / initial_assets

    # 2. Annualized Return (CAGR)
    days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
    if days > 0:
        cagr = (final_assets / initial_assets) ** (365.0 / days) - 1.0
    else:
        cagr = 0.0

    # 3. Max Drawdown
    # Calculate cumulative max assets
    df['cummax'] = df['total_assets'].cummax()
    df['drawdown'] = (df['total_assets'] - df['cummax']) / df['cummax']
    max_drawdown = df['drawdown'].min()

    # 4. Sharpe Ratio
    # Assume risk-free rate = 3% per year -> daily Rf approx 0.03/252
    rf_daily = 0.03 / 252.0
    excess_returns = df['daily_return'] - rf_daily
    if df['daily_return'].std() > 0:
        sharpe_ratio = (excess_returns.mean() / df['daily_return'].std()) * np.sqrt(252)
    else:
        sharpe_ratio = 0.0

    # 5. Win Rate
    win_rate = 0.0
    total_trades = 0
    if trades_df is not None and not trades_df.empty:
        # Filter for SELL actions (completed trades)
        sells = trades_df[trades_df['action'] == 'SELL']
        if not sells.empty:
            # We need to know the profit of each trade.
            # trades_df has 'net_revenue' for SELL and 'total_cost' for BUY.
            # But matching them is complex if we have partial fills or multiple buys.
            # Simplified assumption for "All-in Rotation":
            # 1 Buy followed by 1 Sell.
            # Or we can just use the daily curve? No, that's not "Trade Win Rate".

            # Let's try to match trades by symbol if possible, or just look at 'net_revenue' vs 'cost'?
            # For a rotation strategy, we usually Sell entire position.
            # So Revenue - Cost of previous Buy = Profit.

            # Let's iterate and match?
            # Or simplified: A trade is "Winning" if price_sell > price_buy?
            # (ignoring comms for a moment? No, "net" is better).

            # Let's reconstruct PnL from the sequence: Buy A, Sell A (Profit), Buy B, Sell B...
            # Since we only hold 1 stock at a time:
            # Collect list of (Buy Cost, Sell Revenue) pairs.

            pnl_list = []
            current_buy_cost = 0.0

            # Sort by date
            sorted_trades = trades_df.sort_values('date')

            for _, trade in sorted_trades.iterrows():
                if trade['action'] == 'BUY':
                    current_buy_cost += trade['total_cost']
                elif trade['action'] == 'SELL':
                    revenue = trade['net_revenue']
                    # Approximation: Compare with accumulated cost
                    if current_buy_cost > 0:
                        profit = revenue - current_buy_cost
                        pnl_list.append(profit)
                        current_buy_cost = 0.0 # Reset for next rotation

            if pnl_list:
                wins = sum(1 for p in pnl_list if p > 0)
                total_trades = len(pnl_list)
                win_rate = wins / total_trades if total_trades > 0 else 0.0

    return {
        "total_return_pct": total_return * 100,
        "cagr_pct": cagr * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "sharpe_ratio": sharpe_ratio,
        "win_rate_pct": win_rate * 100,
        "total_trades": total_trades
    }

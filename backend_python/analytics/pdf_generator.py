import pandas as pd
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

def generate_quant_charts(df: pd.DataFrame):
    if df is None or df.empty:
        return None
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
    
    df_clean = df[df['status'].isin(['WIN', 'LOSS'])].copy()
    if df_clean.empty:
        return None

    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(3, 1, figsize=(10, 15))
    plt.subplots_adjust(hspace=0.4)

    hourly_stats = df_clean.groupby('hour').agg(
        count=('status', 'count'),
        wins=('status', lambda x: (x == 'WIN').sum())
    ).reset_index()
    hourly_stats['winrate'] = (hourly_stats['wins'] / hourly_stats['count']) * 100
    
    all_hours = pd.DataFrame({'hour': range(24)})
    hourly_stats = pd.merge(all_hours, hourly_stats, on='hour', how='left').fillna(0)

    ax1 = axes[0]
    sns.barplot(data=hourly_stats, x='hour', y='count', ax=ax1, color='steelblue', alpha=0.7)
    ax1.set_ylabel('Signals Count')
    ax1.set_title('Распределение сигналов и Winrate по часам (UTC)')
    
    ax2 = ax1.twinx()
    sns.lineplot(data=hourly_stats, x='hour', y='winrate', ax=ax2, color='red', marker='o')
    ax2.set_ylabel('Winrate (%)')
    ax2.set_ylim(0, 105)

    ax3 = axes[1]
    if 'volume_24h' in df_clean.columns and 'time_to_take_mins' in df_clean.columns:
        sns.scatterplot(data=df_clean, x='volume_24h', y='time_to_take_mins', hue='status', 
                        palette={'WIN': 'green', 'LOSS': 'red'}, ax=ax3, alpha=0.6)
        ax3.set_xscale('log')
        ax3.set_title('Объем 24h vs Время до Тейк-Профита')

    ax4 = axes[2]
    df_sorted = df_clean.sort_values('timestamp')
    df_sorted['pnl'] = df_sorted['status'].apply(lambda x: 1 if x == 'WIN' else -1)
    df_sorted['equity'] = df_sorted['pnl'].cumsum()
    
    sns.lineplot(data=df_sorted, x=range(len(df_sorted)), y='equity', ax=ax4, color='purple')
    ax4.fill_between(range(len(df_sorted)), df_sorted['equity'], alpha=0.3, color='purple')
    ax4.set_title('Динамика эффективности (Win/Loss Equity)')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    return buf
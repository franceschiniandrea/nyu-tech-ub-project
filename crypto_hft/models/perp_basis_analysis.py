# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
import polars as pl
import matplotlib.pyplot as plt
import statsmodels.api as sm
from pathlib import Path

# %% [markdown]
# ## Importing Data

# %%
base_dir = Path('../../data')
spot = pl.scan_parquet(base_dir.joinpath('data_export.pq')).drop('__index_level_0__').filter(pl.col('symbol').eq('sol-usdt'))
funding = pl.scan_parquet(base_dir.joinpath('funding_sol-usdt-perp_binance_futures_2024-01-01_2025-04-15.parquet'))
perp = pl.scan_parquet(base_dir.joinpath('level_1_sol-usdt-perp_binance_futures_2025-01-01_2025-04-15.parquet')).with_columns(
    midprice=(pl.col('ask_0_price').add(pl.col('bid_0_price'))) / 2
)

# %%
perp.head().collect()

# %%
spot.head().collect()

# %%
funding.head().collect()

# %%
prices = pl.concat([df.select(sorted(df.columns)) for df in [spot, perp]], how='vertical_relaxed')

prices.head().collect(engine='streaming')

# %%
prices.select('symbol').unique().collect(engine='streaming')

# %%
prices.select('exchange').unique().collect(engine='streaming')

# %%
mids = prices.with_columns(pl.col('origin_time').dt.truncate('5s')).group_by('origin_time', 'symbol').agg(
    pl.col('midprice').last().alias('close'),
    pl.col('midprice').first().alias('open'),
).sort('origin_time')

mids.head().collect(engine='streaming')

# %%
index = pl.col("origin_time")
on = pl.col("symbol")
values = pl.col("close")
unique_column_values = ["sol-usdt", 'SOL-USDT-PERP']

# aggregate_function = lambda col: col.list.first()

mids_pivoted = mids.lazy().group_by(index).agg(
    values.filter(on == value).alias(value)
    for value in unique_column_values
).sort(index)

mids_pivoted = mids_pivoted.select(
    pl.col('origin_time'),
    pl.col('sol-usdt').list.first().alias('sol-usdt-spot'), 
    pl.col('SOL-USDT-PERP').list.first().alias('sol-usdt-perp'),
)

# %%
mids_pivoted.head().collect(engine='streaming')

# %%
data = mids_pivoted.join_asof(
    funding.sort('origin_time').select('origin_time', pl.col('rate').alias('funding_rate')),
    on='origin_time',
    strategy='backward',
).with_columns(
    ((pl.col('sol-usdt-perp').sub(pl.col('sol-usdt-spot'))) / (pl.col('sol-usdt-spot')) * (1e4)).alias('basis'),
    funding_rate_ann=pl.col('funding_rate') * 3 * 252
)

data.head().collect(engine='streaming')

# %% [markdown]
# ## Some Visualization

# %%
plot_data = data.filter(
    pl.col('origin_time').dt.date() == pl.lit('2025-02-06').str.to_date()
).collect(engine='streaming')

plot_data.head()

# %%
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7,10))

# ax1.plot(plot_data['origin_time'], plot_data['btc-usdt-spot'], label='Spot')
# ax1.plot(plot_data['origin_time'], plot_data['btc-usdt-perp'], label='Perpetual')

ax1.plot(plot_data['origin_time'], plot_data['funding_rate_ann'])
ax1.set_title('funding rate (annualized)')

ax2.plot(plot_data['origin_time'], plot_data['basis'], label='Basis (bps)')
ax2.set_title('Basis (bps)')

# %% [markdown]
# ## Modelling and stuff

# %%
regdata = plot_data.drop_nulls()

# %%
d = regdata.with_columns(
    # 4h half-life = 4 * 60 * 60 seconds / 5 seconds
    (pl.col('funding_rate_ann') - pl.col('funding_rate_ann').ewm_mean(half_life=12 * 60)).alias('funding_rate_ann_demeaned'),
)

plt.plot(d['origin_time'], d['funding_rate_ann_demeaned'], label='funding rate (ewm)')
plt.plot(d['origin_time'], d['funding_rate_ann'], label='funding rate (ewm)')

# %%
d = regdata.with_columns(
    # 4h half-life = 4 * 60 * 60 seconds / 5 seconds
    (pl.col('basis') - pl.col('basis').ewm_mean(half_life=12 * 60)).alias('basis_demeaned'),
)

plt.plot(d['origin_time'], d['basis_demeaned'], label='funding rate (ewm)')
plt.plot(d['origin_time'], d['basis'], label='funding rate (ewm)')

# %%
regdata = data.with_columns(
    (pl.col('funding_rate_ann') - pl.col('funding_rate_ann').ewm_mean(half_life=12 * 60, min_samples=12)).alias('funding_rate_ann_demeaned'),
    (pl.col('basis') - pl.col('basis').ewm_mean(half_life=12 * 60, min_samples=12)).alias('basis_demeaned'),
).drop_nulls().sort('origin_time')

regdata.head().collect(engine='streaming')

# %%
regdata = regdata.collect(engine='streaming')

# %% [markdown]
# First - there most likely is a two-way relationship between the basis vs the funding rate
#
# First, basis ~ funding_rate

# %%
# first model - basis vs funding rate at t - 3 (15 seconds before)
model = sm.OLS(
    regdata['basis_demeaned'].to_pandas(), 
    sm.add_constant(regdata['funding_rate_ann_demeaned'].shift(3).to_pandas()),
    missing='drop'
).fit() 

model.summary()

# %%
resid_basis_vs_funding = model.resid
plt.plot(resid_basis_vs_funding[-1500:]);

# %%
import statsmodels.api as sm

# first model - basis vs funding rate at t - 3 (15 seconds before)
model = sm.OLS(
    regdata['funding_rate_ann_demeaned'].to_numpy(), 
    sm.add_constant(regdata['basis_demeaned'].shift(3).to_numpy()),
    missing='drop'
).fit() 

model.summary()

# %%
plt.plot(model.resid);

# %% [markdown]
# Now, the residuals of the basis vs the funding rate might be predictive of the change in the midprice - that is because if the basis moves more or less than the direction the funding rate wanted, it signifies some other flow that is undermining the goal of funding rate (bring the basis to zero)

# %%
regdata['sol-usdt-perp'].log().head()

# %% [markdown]
# this is the good stuff

# %%
# regression of 15s changes in midprice vs de-meaned basis
model = sm.OLS(
    regdata['sol-usdt-perp'].log().diff(6).to_pandas() * 1e4, 
    sm.add_constant(regdata['basis_demeaned'].shift(6).to_pandas()),
    missing='drop'
).fit()

model.summary()


# %%
def get_r2(lag): 
    model = sm.OLS(
        regdata['sol-usdt-perp'].log().diff(lag).to_pandas() * 1e4, 
        sm.add_constant(regdata['basis_demeaned'].shift(lag).to_pandas()),
        missing='drop'
    ).fit()

    model.summary()

    return model.rsquared

res = {f'l_{i}': get_r2(i) for i in range(1, 15)}
res = pl.DataFrame(res)

res.head()

# %%
plt.bar(res.columns, res.to_numpy()[0])

# %%
plt.plot(model.resid)

# %% [markdown]
# this is other stuff I did to understand basis vs funding rate but have to make more sense of it

# %%
import statsmodels.api as sm

regdata = plot_data.drop_nulls()

model_basis_funding = sm.OLS(
    regdata['basis'].to_numpy(), 
    sm.add_constant(regdata['funding_rate'].to_numpy()),
    missing='drop'
).fit() 

model.summary()

# %%
plt.plot(model.resid)

# %% [markdown]
# ok, there seems to be a 2-way relationship (quite intuitive) between them.

# %%
plt.plot(regdata['origin_time'], regdata['funding_rate_ann'])

# %%
# let's compare avg performance over the past 15m vs funding rate
regdata.head()

# %%
93678.015 * (1 + 0.0001)

# %% [markdown]
# this is another idea I was testing - if you see the funding rate as the avg cost longs pay to keep the position open, if the return over the funding period starts to diverge from the funding rate the losing side will have to close eventually. 
#
# doesn't seem very present in this data but imo it's because the funding rates are so low (and not very volatile) on binance that it doesn't really matter when the ccy moves 20bps in an hour if the funding rate is 1 or 2bps. Would be interesting to test this on shitcoins and on other exchanges where liquidity is not as good and funding rates can get crazy. 

# %%
# compare the avg cost of funding the position with the past return
regdata = regdata.with_columns(
    (pl.col('funding_rate') * 1e4 / 60 / 8).rolling_mean(12).alias('avg_funding_rate_1min'),
    pl.col('sol-usdt-spot').log().diff().mul(1e4).rolling_mean(12).alias('log_return_1min')
)

plt.plot(regdata['origin_time'], regdata['avg_funding_rate_1min'], label='avg funding rate')
ax2 = plt.twinx()

ax2.plot(regdata['origin_time'], regdata['log_return_1min'], label='avg log return', color='red')

# %%
regdata = regdata.with_columns(
    (pl.col('log_return_1min') - pl.col('avg_funding_rate_1min')).alias('1min_dislocation'),
)

start_i = 2000
i = 12 * 60 # 8h
plt.plot(regdata['origin_time'][start_i:start_i+i], regdata['1min_dislocation'][start_i:start_i+i])

ax2 = plt.twinx()
ax2.plot(regdata['origin_time'][start_i:start_i+i], regdata['sol-usdt-spot'][start_i:start_i+i], color='red', label='spot price')

plt.legend()

# %%
model = sm.OLS(
    (regdata['sol-usdt-spot'].log().diff(6) * 1e4).to_numpy(), 
    sm.add_constant(regdata['1min_dislocation'].shift(6).to_numpy()),
    missing='drop'
).fit()

model.summary()

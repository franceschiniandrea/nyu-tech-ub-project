# Machine Learning Systems Overview

## Models Overview 

### Cross-Currency Exchange Arb

The main idea is simple - liquidity is fragmented across venues in crypto, and there are some books which are so illiquid that you can hit one side on the illiquid exchange, transfer the asset to a liquid exchange, and hit the other side there. 

The pure taker/taker arbitrage is almost never possible, since it is arbitraged away almost immediately. But by relaxing the strategy to maker/taker, there are some nice opportunities that do not go away even after controlling for TCs and transfer costs across wallets. 

The arb would go as follows for the buy illiquid, sell liquid side - the opposite can be done, but is slightly more challenging, since you need to first buy on the liquid exchange, transfer, and sell on the illiquid exchange, and the arb might get faded away in the meantime.

* Scan for opportunities that are EV positive after controlling for TCs + transfer costs
* Start making on the illiquid book, and start building inventory
* Once you have enough inventory transfer to the liquid exchange and close the position
* Transfer back the liquidity to the illiquid exchange and start making again

Why does this exist? 

* Liquidity in crypto is fragmented, but most importantly, it is sticky. It is not as easy as in tradfi to move assets from one exchange to the other, which can cause some dislocations (such as this one) to persist over time
* In a sense, the service you are providing to the market is to make liquidity flow from the more liquid books to the illiquid ones
* A good execution engine would spread an order across multiple exchanges relative to the liquidity on each one; the reality is that many participants in this market do not have the infrastructure or the capabilities to do so, and end up executing sub-optimally (thus creating this dislocations)

### Fast Trend-Following

Trend following is a thoroughly researched strategy in the tradfi space (and these days, one could argue in crypto as well) - although trend is usually harvested at much lower frequencies, we want to see whether there is some trending behaviour in the high frequency space that one could exploit as an alpha signal while making.

Why do we think this exists? 

* It's well researched in the tradfi space, which is a good start
* There are a lot of participants in crypto that are rather price insensitive and there is more hoarding behaviour than in the tradfi space, which should help
* High vol and overall uncertainty contributes to liquidation events and other events which force participants to not execute optimally and to create self-reinforcing loops.

Since it's hard to borrow spot in crypto markets, we are going to focus on perpetual futures for this analysis - they are very liquid and an easy way to take both sides of the market. 

We want to base our analysis on a simple (yet robust) modelling approach, restricting ourselves to linear models and using signals from traditional markets, such as order book imbalance, breakouts, liquidations. Since we are using perpetual futures, this also allows us to analyze the effect of funding rates on the strategy. 

### Data Pipeline Readiness
```mermaid
graph LR
    A[Raw WebSocket Data] --> B[MySQL Database]
    B --> C[Feature Engineering]
    C --> D[Model Training]
    D --> E[Real-time Inference]
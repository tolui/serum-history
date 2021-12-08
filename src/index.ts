import { Account, Commitment, Connection, PublicKey } from '@solana/web3.js'
import { Market } from '@project-serum/serum'
import cors from 'cors'
import express from 'express'
import { Tedis, TedisPool } from 'tedis'
import { URL } from 'url'
import { decodeRecentEvents } from './events'
import { MarketConfig, Trade, TradeSide } from './interfaces'
import { RedisConfig, RedisStore, createRedisStore } from './redis'
import { resolutions, sleep } from './time'
// @ts-ignore: Unreachable code error
import {
  Config,
  getMarketByBaseSymbolAndKind,
  GroupConfig,
  MangoClient,
  PerpMarketConfig,
  FillEvent,
} from '@blockworks-foundation/mango-client'
import BN from 'bn.js'
import notify from './notify'
import LRUCache from 'lru-cache'
import * as dayjs from 'dayjs'

const redisUrl = new URL(process.env.REDISCLOUD_URL || 'redis://localhost:6379')
const host = redisUrl.hostname
const port = parseInt(redisUrl.port)
let password: string | undefined
if (redisUrl.password !== '') {
  password = redisUrl.password
}

const network = 'mainnet-beta'
const clusterUrl =
  process.env.RPC_ENDPOINT_URL || 'https://solana-api.projectserum.com'
  //process.env.RPC_ENDPOINT_URL || 'https://api.mainnet-beta.solana.com'
const fetchInterval = process.env.INTERVAL ? parseInt(process.env.INTERVAL) : 30

console.log({ clusterUrl, fetchInterval })
console.log("dex.mn 2021-12-08")

const programIdV3 = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'

const nativeMarketsV3: Record<string, string> = {
  "QUEST/USDT": "7QwEMFeKS8mPACndc9EzpgoqKbQhpBm1N4JCtzjGEyR7",
  "ARDX/SOL": "9uPmBwdNnfWc2fzvTQ264urJBY1JChrGcmhUeojzNfyW",
  "BTC/USDT": "C1EuT9VokAKLiW7i2ASnZUvxDoKuKkCpDDeNxAptuNe4",
  "BTC/USDC": "A8YFbxQYFVqKZaoYJLLUVcQiWP7G2MeEgW5wsAQgMvFw",
  "ETH/USDT": "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
  "ETH/USDC": "4tSvZvnbyzHXLMTiFonMyxZoHmFqau1XArcRCVHLZ5gX",
  "SOL/USDT": "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
  "SOL/USDC": "9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT",
  "SRM/USDT": "AtNnsY1AyRERWJ8xCskfz38YdvruWVJQUVXgScC1iPb",
  "SRM/USDC": "ByRys5tuUWDgL73G8JBAEfkdFf8JWBzPBDHsBVQ5vbQA",
  "RAY/USDT": "teE55QrL4a4QSfydR9dnHF97jgCfptpuigbb53Lo95g",
  "RAY/USDC": "2xiv8A5xrJ7RnGdxXB42uFEkYHJjszEhaJyKKt4WaLep",
  "USDT/USDC":"77quYg4MGneUdjgXCunt9GgM1usmrxKY31twEy3WHwcS",
  "KIN/USDT":"4nCFQr8sahhhL4XJ7kngGFBmpkmyf3xLzemuMhn6mWTm",
  "KIN/USDC":"Bn6NPyr6UzrFAwC4WmvPvDr2Vm8XSUnFykM2aQroedgn",
  "ATLAS/USDC":"Di66GTLsV64JgCCYGVcY21RZ173BHkjJVgPyezNN7P1K"
}

const symbolsByPk = Object.assign(
  {},
  ...Object.entries(nativeMarketsV3).map(([a, b]) => ({ [b]: a }))
)

async function collectEventQueue(m: MarketConfig, r: RedisConfig) {
  try {
    const store = await createRedisStore(r, m.marketName)
    const marketAddress = new PublicKey(m.marketPk)
    const programKey = new PublicKey(m.programId)
    const connection = new Connection(m.clusterUrl)
    const market = await Market.load(
      connection,
      marketAddress,
      undefined,
      programKey
    )

    async function fetchTrades(
      lastSeqNum?: number
    ): Promise<[Trade[], number]> {
      const now = Date.now()
      const accountInfo = await connection.getAccountInfo(
        market['_decoded'].eventQueue
      )
      if (accountInfo === null) {
        throw new Error(
          `Event queue account for market ${m.marketName} not found`
        )
      }
      const { header, events } = decodeRecentEvents(
        accountInfo.data,
        lastSeqNum
      )
      const takerFills = events.filter(
        (e) => e.eventFlags.fill && !e.eventFlags.maker
      )
      const trades = takerFills
        .map((e) => market.parseFillEvent(e))
        .map((e) => {
          return {
            price: e.price,
            side: e.side === 'buy' ? TradeSide.Buy : TradeSide.Sell,
            size: e.size,
            ts: now,
          }
        })
      /*
    if (trades.length > 0)
      console.log({e: events.map(e => e.eventFlags), takerFills, trades})
    */
      return [trades, header.seqNum]
    }

    async function storeTrades(ts: Trade[]) {
      if (ts.length > 0) {
        console.log(m.marketName, ts.length)
        for (let i = 0; i < ts.length; i += 1) {
          await store.storeTrade(ts[i])
        }
      }
    }

    while (true) {
      try {
        const lastSeqNum = await store.loadNumber('LASTSEQ')
        const [trades, currentSeqNum] = await fetchTrades(lastSeqNum)
        storeTrades(trades)
        store.storeNumber('LASTSEQ', currentSeqNum)
      } catch (e:any) {
        notify(`collectEventQueue ${m.marketName} ${e.toString()}`)
      }
      await sleep({ Seconds: fetchInterval })
    }
  } catch (e:any) {
    notify(`collectEventQueue ${m.marketName} ${e.toString()}`)
  }
}

function collectMarketData(programId: string, markets: Record<string, string>) {
  if (process.env.ROLE === 'web') {
    console.warn('ROLE=web detected. Not collecting market data.')
    return
  }

  Object.entries(markets).forEach((e) => {
    const [marketName, marketPk] = e
    const marketConfig = {
      clusterUrl,
      programId,
      marketName,
      marketPk,
    } as MarketConfig
    collectEventQueue(marketConfig, { host, port, password, db: 0 })
  })
}

if (process.env.pm_id === '11' || !process.env.pm_id) {
  collectMarketData(programIdV3, nativeMarketsV3);
}

const groupConfig = Config.ids().getGroup('mainnet', 'mainnet.1') as GroupConfig

async function collectPerpEventQueue(r: RedisConfig, m: PerpMarketConfig) {
  const connection = new Connection(clusterUrl, 'processed' as Commitment)

  const store = await createRedisStore(r, m.name)
  const mangoClient = new MangoClient(connection, groupConfig!.mangoProgramId)
  const mangoGroup = await mangoClient.getMangoGroup(groupConfig!.publicKey)
  const perpMarket = await mangoGroup.loadPerpMarket(
    connection,
    m.marketIndex,
    m.baseDecimals,
    m.quoteDecimals
  )

  async function fetchTrades(lastSeqNum?: BN): Promise<[Trade[], BN]> {
    lastSeqNum ||= new BN(0)
    const now = Date.now()

    const eventQueue = await perpMarket.loadEventQueue(connection)
    const events = eventQueue.eventsSince(lastSeqNum)

    const trades = events
      .map((e) => e.fill)
      .filter((e) => !!e)
      .map((e) => perpMarket.parseFillEvent(e))
      .map((e) => {
        return {
          price: e.price,
          side: e.takerSide === 'buy' ? TradeSide.Buy : TradeSide.Sell,
          size: e.quantity,
          ts: e.timestamp.toNumber() * 1000,
        }
      })

    if (events.length > 0) {
      const last = events[events.length - 1]
      const latestSeqNum =
        last.fill?.seqNum || last.liquidate?.seqNum || last.out?.seqNum
      lastSeqNum = latestSeqNum
    }

    return [trades, lastSeqNum as BN]
  }

  async function storeTrades(ts: Trade[]) {
    if (ts.length > 0) {
      console.log(m.name, ts.length)
      for (let i = 0; i < ts.length; i += 1) {
        await store.storeTrade(ts[i])
      }
    }
  }

  while (true) {
    try {
      const lastSeqNum = await store.loadNumber('LASTSEQ')
      const [trades, currentSeqNum] = await fetchTrades(new BN(lastSeqNum || 0))
      storeTrades(trades)
      store.storeNumber('LASTSEQ', currentSeqNum.toString() as any)
    } catch (err:any) {
      notify(`collectPerpEventQueue ${m.name} ${err.toString()}`)
    }

    await sleep({ Seconds: fetchInterval })
  }
}

if (process.env.ROLE === 'web') {
  console.warn('ROLE=web detected. Not collecting perp market data.')
} else {
  // groupConfig.perpMarkets.forEach((m) =>
  //   collectPerpEventQueue({ host, port, password, db: 0 }, m)
  // )
}

const cache = new LRUCache<string, Trade[]>(
  parseInt(process.env.CACHE_LIMIT ?? '500')
)

const marketStores = {} as any

const priceScales: any = {
  "QUEST/USDT": 10000,
  "ARDX/SOL": 1000,
  "BTC/USDT": 1,
  "BTC/USDC": 1,
  "ETH/USDT": 10,
  "ETH/USDC": 10,
  "SOL/USDT": 1000,
  "SOL/USDC": 1000,
  "SRM/USDT": 1000,
  "SRM/USDC": 1000,
  "RAY/USDT": 1000,
  "RAY/USDC": 1000,
  "USDT/USDC": 1000,
  "KIN/USDT": 10000,
  "KIN/USDC": 10000,
  "ATLAS/USDC": 1000
}

Object.keys(priceScales).forEach((marketName) => {
  const conn = new Tedis({
    host,
    port,
    password,
  })

  const store = new RedisStore(conn, marketName)
  marketStores[marketName] = store

  // preload heavy markets
  if (['SOL/USDC', 'SOL-PERP', 'BTC-PERP'].includes(marketName)) {
    for (let i = 1; i < 60; ++i) {
      const day = dayjs.default().subtract(i, 'days')
      const key = store.keyForDay(+day)
      store
        .loadTrades(key, cache)
        .then(() => {
          //console.log('loaded', key)
        })
        .catch(() => console.error('could not cache', key))
    }
  }
})

const app = express()
var corsOptions = {
  origin: ['https://dex.mn', 'http://localhost:3000', 'https://trade.paynow.mn', 'https://test.dex.mn'],
  optionsSuccessStatus: 200 // some legacy browsers (IE11, various SmartTVs) choke on 204
}
app.use(cors(corsOptions))

app.get('/tv/config', async (req, res) => {
  const response = {
    supported_resolutions: Object.keys(resolutions),
    supports_group_request: false,
    supports_marks: false,
    supports_search: true,
    supports_timescale_marks: false,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/symbols', async (req, res) => {
  const symbol = req.query.symbol as string
  const response = {
    name: symbol,
    ticker: symbol,
    description: symbol,
    type: 'Spot',
    session: '24x7',
    exchange: 'DEX.mn',
    listed_exchange: 'DEX.mn',
    timezone: 'America/New_York',
    has_intraday: true,
    supported_resolutions: Object.keys(resolutions),
    minmov: 1,
    pricescale: priceScales[symbol] || 10000,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/history', async (req, res) => {
  // parse
  const marketName = req.query.symbol as string
  const market =
    nativeMarketsV3[marketName] ||
    groupConfig.perpMarkets.find((m) => m.name === marketName)
  const resolution = resolutions[req.query.resolution as string] as number
  let from = parseInt(req.query.from as string) * 1000
  let to = parseInt(req.query.to as string) * 1000

  // validate
  const validSymbol = market != undefined
  const validResolution = resolution != undefined
  const validFrom = true || new Date(from).getFullYear() >= 2021
  if (!(validSymbol && validResolution && validFrom)) {
    const error = { s: 'error', validSymbol, validResolution, validFrom }
    console.error({ marketName, error })
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const store = marketStores[marketName] as RedisStore

    // snap candle boundaries to exact hours
    from = Math.floor(from / resolution) * resolution
    to = Math.ceil(to / resolution) * resolution

    // ensure the candle is at least one period in length
    if (from == to) {
      to += resolution
    }
    const candles = await store.loadCandles(resolution, from, to, cache)
    const response = {
      s: 'ok',
      t: candles.map((c) => c.start / 1000),
      c: candles.map((c) => c.close),
      o: candles.map((c) => c.open),
      h: candles.map((c) => c.high),
      l: candles.map((c) => c.low),
      v: candles.map((c) => c.volume),
    }
    res.set('Cache-control', 'public, max-age=1')
    res.send(response)
    return
  } catch (e:any) {
    notify(`tv/history ${marketName} ${e.toString()}`)
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

app.get('/trades/address/:marketPk', async (req, res) => {
  // parse
  const marketPk = req.params.marketPk as string
  const marketName =
    symbolsByPk[marketPk] ||
    groupConfig.perpMarkets.find((m) => m.publicKey.toBase58() === marketPk)
      ?.name

  // validate
  const validPk = marketName != undefined
  if (!validPk) {
    const error = { s: 'error', validPk }
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const store = marketStores[marketName] as RedisStore
    const trades = await store.loadRecentTrades()
    const response = {
      success: true,
      data: trades.map((t) => {
        return {
          market: marketName,
          marketAddress: marketPk,
          price: t.price,
          size: t.size,
          side: t.side == TradeSide.Buy ? 'buy' : 'sell',
          time: t.ts,
          orderId: '',
          feeCost: 0,
        }
      }),
    }
    res.set('Cache-control', 'public, max-age=5')
    res.send(response)
    return
  } catch (e:any) {
    notify(`trades ${marketName} ${e.toString()}`)
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

const httpPort = 5000
app.listen(httpPort)

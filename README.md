# 🚀 V6 Data Extraction System (b6v)

Railway-ready deployment of V6 PancakeSwap prediction data extraction system.

## 🚀 Quick Start

### Local Testing
```bash
# Test the system
node test-crawler.js

# Start historical data crawler
./start-hisbet.sh

# Start realtime data listener  
./start-realbet.sh
```

### Railway Deployment
1. Connect this GitHub repository to Railway
2. Add PostgreSQL database service
3. Deploy automatically with included configuration

## 📊 System Components

### 🔧 Core Services
- **hisbet**: Historical data crawler (`v6-unified-crawler.js`)
- **realbet**: Realtime data listener (`realtime-listener.js`)

### 🔧 Infrastructure
- `TimeService.js` - Unified time handling (Taipei timezone)
- `abi.json` - Smart contract ABI
- `package.json` - Dependencies and scripts
- `railway.json` - Railway deployment config

### 📊 Database Architecture
- **Database**: PostgreSQL 17 (Neon/Railway)
- **Timezone**: Asia/Taipei
- **Tables**: `round`, `hisbet`, `realbet`, `claim`, `multi_claims`

## 🎯 Features

### Historical Data Crawler
- ✅ Dual-thread system (main + support threads)
- ✅ Auto-restart every 30 minutes
- ✅ Data integrity validation
- ✅ Rate limiting (100 req/s)
- ✅ Automatic error recovery

### Realtime Data Listener  
- ✅ WebSocket blockchain monitoring
- ✅ Suspicious wallet detection
- ✅ Duplicate bet prevention
- ✅ Auto-reconnection
- ✅ WebSocket server (port 3010)

## 🔄 Data Flow

```
Historical: Blockchain events → round/hisbet/claim tables → cleanup realbet
Realtime: WebSocket events → realbet table → WebSocket clients
```

## 🚀 Railway Environment Variables

Automatically configured by Railway:
- `DATABASE_URL` - PostgreSQL connection string
- `PORT` - Application port
- `RAILWAY_ENVIRONMENT` - Deployment environment

Optional custom variables:
- `V6_RPC_URL` - Custom RPC node (defaults to drpc.org)
- `V6_DATABASE_URL` - Custom database URL

## 📈 Monitoring

### Health Checks
- Historical crawler: Processes epochs and reports statistics
- Realtime listener: WebSocket connections and bet processing
- Database: Connection status and query performance

### Key Metrics
- 📊 Processed rounds
- 💰 Extracted bets  
- 🏆 Claim records
- 🚨 Suspicious wallets detected
- ❌ Error counts

## 🛠️ Troubleshooting

### Common Issues
1. **Database connection failed**: Check `DATABASE_URL` environment variable
2. **Blockchain connection failed**: Verify RPC node accessibility  
3. **Port conflicts**: Check ports 3008 (hisbet) and 3010 (realbet)

### Logs to Monitor
- 🚀 Startup information
- ✅ Successful data processing
- 📊 Processing statistics
- 🚨 Suspicious activity alerts
- ❌ Error messages and retries

## 📋 Deployment Checklist

- [x] ✅ Railway configuration files
- [x] ✅ Database schema ready
- [x] ✅ Environment variables configured
- [x] ✅ Health monitoring enabled
- [x] ✅ Auto-scaling configured
- [x] ✅ Error recovery implemented

---

**Ready for Railway deployment** 🚂

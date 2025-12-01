# Partition Auto-Init Testing - Quick Start

## Prerequisites

Start Docker Desktop first!

## Automated Testing (Recommended)

```bash
./scripts/test-partition-init.sh
```

This runs all tests automatically and shows detailed results.

## Manual Testing

### 1. Start Cluster

```bash
docker compose up -d
sleep 10
```

### 2. Check Initialization Logs

```bash
# Leader should show initialization
docker logs raft-node-0 | grep -i partition

# Expected output:
# "Partition table is empty, waiting for leader election..."
# "This node is the leader, initializing partition table"
# "Initialized partition table total_partitions=16384 nodes=3 version=1"
# "Updated partition table to version 1 (16384 assignments)"
```

### 3. Verify Partition Table

```bash
curl http://localhost:8080/admin/partition-table | python3 -m json.tool
```

**Expected**: `"assignment_count": 16384`

### 4. Test Operations

```bash
# PUT
curl -X PUT http://localhost:8080/cache/1 \
  -H "Content-Type: application/json" \
  -d '{"value": 42}'
# Expected: {"success": true}

# GET
curl http://localhost:8080/cache/1
# Expected: {"key": 1, "value": 42}

# DELETE
curl -X DELETE http://localhost:8080/cache/1
# Expected: {"success": true}
```

## What Changed

### File Modified
- `/Users/dzc/distributed-cache/cmd/raftnode/main.go`

### Key Changes
1. Added `autoInitializePartitions()` function
2. Launches auto-init as goroutine after Raft starts
3. Leader initializes partition table with 16,384 assignments
4. Replicates to all followers via Raft consensus

## Troubleshooting

### Docker not running
```bash
# Check Docker status
docker info

# If error, start Docker Desktop application
```

### Cluster won't start
```bash
# Clean up and retry
docker compose down -v
docker compose up -d
```

### No partition table assignments
```bash
# Check all node logs
docker compose logs | grep -i partition

# Look for initialization errors
```

### Operations still failing
```bash
# Verify partition table has assignments
curl http://localhost:8080/admin/partition-table

# Should show assignment_count: 16384
# If 0, check leader logs for errors
```

## Documentation

- **Full Details**: `claudedocs/partition-auto-init-implementation.md`
- **Summary**: `claudedocs/IMPLEMENTATION_SUMMARY.md`
- **Bug Analysis**: `claudedocs/sharding-partition-bug-analysis.md`

## Success Criteria

All of these should pass:

- ✓ Docker is running
- ✓ 3 containers start successfully
- ✓ Leader logs show initialization
- ✓ Partition table has 16,384 assignments
- ✓ PUT operation succeeds
- ✓ GET operation succeeds
- ✓ DELETE operation succeeds
- ✓ Cluster restart preserves table

## Need Help?

Check logs for any errors:
```bash
docker compose logs
```

Or run the automated test script for detailed diagnostics:
```bash
./scripts/test-partition-init.sh
```

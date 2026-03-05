<div align="center">

![SyndrDB Logo](/logo.png)

# SyndrDB

### **The Database Revolution You've Been Waiting For** 🚀

*MongoDB's flexibility.  PostgreSQL's power. GraphQL's elegance. One server.*

[![Status](https://img.shields.io/badge/status-MVP%20Ready-success)](https://www.syndrdb.com)
[![License](https://img.shields.io/badge/license-BSL%201.1-blue)](./LICENSE)
[![Go](https://img.shields.io/badge/Go-99.9%25-00ADD8?logo=go)](https://golang.org)
[![Version](https://img.shields.io/badge/version-0.0.1alpha-orange)]()

[Website](https://www.syndrdb.com) • [Documentation](https://www.syndrdb.com/docs) • [Community](https://www.syndrdb.com/community)

---

</div>

## 💡 Why SyndrDB?

**The Problem:** Modern backend development is unnecessarily complex. You need MongoDB for flexibility, PostgreSQL for relationships, GraphQL servers for APIs, ORMs to bridge the gaps...  It's a maze of dependencies, configuration, and overhead.

**The Solution:** SyndrDB brings it all together in **one powerful, zero-dependency server** that speaks both SyndrQL (our SQL-inspired language) and GraphQL natively.

### ✨ What Makes SyndrDB Different

```
🎯 Relational Document Database    → Stored as flexible JSON documents with strict relationships
⚡ Native GraphQL Interface         → No separate GraphQL server needed
🔥 Zero ORM Required               → Query directly, get JSON responses
💪 PostgreSQL-Inspired Performance → Cost-based optimizer, smart indexing, SIMD acceleration
🛡️ ACID Compliant                  → Full transactions with Write-Ahead Logging
🚀 Single Binary Deployment        → No dependencies, no setup complexity
📦 Built-in Migrations             → Version control for your database schema
🔒 Enterprise Security             → RBAC, rate limiting, query complexity analysis
```

---
## Current Version: Pre-alpha MVP of Community Edition

---

## 🎬 Quick Start

### Build from Source

```bash
# Clone the repository
git clone https://github.com/dan-strohschein/SyndrDB. git
cd SyndrDB

# Build the server (creates bin/server/server)
./build.sh

# Build and run tests
./build.sh test
```

### Start the Server

```bash
# Start with defaults (TCP on 1776, no CLI flags needed)
./bin/server/server

# Or customize as needed
./bin/server/server -datadir=./my_data -port=1776 -graphql
```

**That's it!** 🎉 SyndrDB is now running and ready for connections. 

> 📚 For complete documentation, deployment guides, and tutorials, visit **[www.syndrdb.com](https://www.syndrdb.com)**

---

## 🚀 See It In Action

### SyndrQL: SQL That Feels Natural

```sql
-- Create a database
CREATE DATABASE "MyApp";

-- Create a bundle (think: table meets collection)
CREATE BUNDLE "users"
WITH FIELDS (
    {"name", STRING, true, false, ""},
    {"email", STRING, true, true, ""},
    {"age", INT, false, false, 0},
    {"is_active", BOOL, false, false, true}
);

-- Add documents
ADD DOCUMENT TO BUNDLE "users"
WITH (
    {"name" = "Alice Johnson"},
    {"email" = "alice@example.com"},
    {"age" = 28}
);

-- Query with powerful WHERE clauses
SELECT name, email, age 
FROM "users" 
WHERE (age >= 25 AND is_active == true)
ORDER BY name ASC
LIMIT 10;

-- JOIN across bundles (with relationships!)
SELECT u.name, o.total, o.status
FROM "users" u
JOIN "orders" o ON u.id == o.user_id
WHERE o.status == "pending"
ORDER BY o.created_at DESC;
```

### GraphQL: Modern API Out of the Box

```graphql
# Query your data
query {
  users(where: "age >= 25", limit: 10) {
    edges {
      node {
        id
        name
        email
        orders {
          total
          status
        }
      }
    }
  }
}

# Mutations work too
mutation {
  createUser(input: {
    name: "Bob Smith"
    email: "bob@example.com"
    age: 32
  }) {
    id
    name
  }
}
```

**No GraphQL server setup.  No schema stitching. It just works.**

---

## 🔥 Power Features

<table>
<tr>
<td width="50%">

### 💾 **Smart Storage**
- PostgreSQL-inspired file format
- B-Tree and Hash indexes
- Cost-based query optimizer
- Automatic index selection
- Hot key detection & caching

</td>
<td width="50%">

### ⚡ **Blazing Performance**
- SIMD-accelerated queries (4-8x faster)
- Async Write-Ahead Logging
- Query result streaming
- DataLoader for N+1 prevention
- 110K+ QPS on commodity hardware

</td>
</tr>
<tr>
<td width="50%">

### 🔒 **Enterprise Security**
- Role-Based Access Control (RBAC)
- Multi-layer rate limiting
- Query complexity analysis
- Session management
- Argon2id password hashing
- Comprehensive audit logging

</td>
<td width="50%">

### 🛠️ **Developer Joy**
- SQL-inspired query language
- Native GraphQL support
- Built-in migration system
- Backup & restore with compression
- JSON responses for everything
- Comprehensive error messages

</td>
</tr>
</table>

---

## 📊 Real-World Performance

```
Hash Index Lookups:     110,000 QPS  (89x faster than full scan)
B-Tree Range Queries:   45,000 QPS   (with SIMD acceleration)
Complex JOINs:          12,000 QPS   (multi-table with WHERE clauses)
GraphQL Queries:        22,000 QPS   (with DataLoader optimization)
```

*Benchmarked on Apple M3 Pro (ARM64).  Your mileage may vary.*

---

## 🏗️ Architecture Philosophy

SyndrDB borrows battle-tested concepts from the best databases:

| From PostgreSQL | From MongoDB | From GraphQL |
|----------------|--------------|--------------|
| ✓ B-Tree indexes | ✓ Flexible schemas | ✓ Type-safe APIs |
| ✓ Query planner | ✓ Document storage | ✓ Field selection |
| ✓ ACID transactions | ✓ JSON responses | ✓ Nested queries |
| ✓ Cost-based optimization | ✓ Horizontal scalability* | ✓ Introspection |

**Then we added:** SIMD acceleration, smart caching, migration versioning, and native GraphQL—all in pure Go with zero external dependencies.

> *Cluster mode, replication, and other advanced features coming in the enterprise version

---

## 🎯 Perfect For

- 🌐 **Modern Web Apps:** Get GraphQL without the complexity and no ORM
- 📱 **Mobile Backends:** Single endpoint, JSON responses, flexible schema
- 🚀 **Startups:** Move fast without sacrificing data integrity
- 🔬 **Prototypes:** Full power without the setup overhead
- 🏢 **Microservices:** Embedded database with API built-in

---

## 🗺️ Roadmap to Community Edition v1.0

- [x] Core SyndrQL query language
- [x] GraphQL interface
- [x] ACID compliance with WAL
- [x] B-Tree and Hash indexes
- [x] Relationships (1-to-Many, Many-to-Many)
- [x] RBAC security system
- [x] Migration system
- [x] Backup & restore

## Enterprise Edition Features

SyndrDB Enterprise extends the Community Edition with production-grade capabilities. All features are implemented as extensions that plug into the core extension system — the core database remains unchanged.

### Implemented

- [x] **Data Governance** — Field classification, dynamic data masking (5 modes), unified audit trail with chain hashing
- [x] **Encryption at Rest** — AES-256-GCM for storage, WAL, and backups with pluggable KMS and key rotation
- [x] **Field-Level Encryption** — Per-field AES encryption for PII/PHI with deterministic and randomized modes
- [x] **Full-Text Search** — BM25-scored inverted index with boolean, phrase, prefix, fuzzy, and proximity queries
- [x] **Temporal Tables** — System-versioned bundles with AS OF / BETWEEN / ALL queries and retention policies
- [x] **High Availability** — Leader-follower replication with automatic failover and read routing
- [x] **Multi-Primary CRDT Replication** — Write-anywhere clusters using LWW-Register, OR-Set, and PN-Counter CRDTs
- [x] **Parallel Query Execution** — Partitioned scatter-gather for large SELECT queries
- [x] **Query Governor** — Concurrent query tracking, kill query, per-user resource limits
- [x] **Prometheus Metrics** — Counters, gauges, histograms with HTTP /metrics endpoint
- [x] **Document-Level Security** — Row-level security policies with per-user/role filtering
- [x] **Change Data Capture** — Async event dispatch with webhook delivery and exactly-once watermarks
- [x] **Vector Search** — HNSW index with cosine, euclidean, and dot product distance metrics
- [x] **Adaptive Query Optimization** — Runtime feedback with automatic plan invalidation
- [x] **Materialized View Refresh** — Incremental change tracking for materialized views
- [x] **Database Migration** — Zero-downtime import from MySQL (mysqldump), MongoDB (EJSON), and MS SQL Server (T-SQL/BACPAC)
- [x] **Range-Based Sharding** — Horizontal data partitioning with transparent write/read routing
- [x] **Cross-Shard Query Routing** — Scatter-gather SELECT with sort, limit, distinct, and aggregate merge
- [x] **Distributed Transactions (2PC)** — Cross-shard atomicity with implicit multi-shard transaction wrapping
- [x] **Online Schema Changes** — Non-blocking ALTER BUNDLE with shadow bundle + CDC catch-up (gh-ost pattern)
- [x] **In-Memory Columnar Processing** — Vectorized aggregation with dictionary encoding and segment pruning
- [x] **Query Result Caching** — Sharded LRU cache with CDC-driven invalidation and proactive warming
- [x] **Geospatial Indexing** — R-tree spatial indexes with ST_DISTANCE, ST_WITHIN, ST_CONTAINS, ST_INTERSECTS, ST_DWITHIN
- [x] **Automated Index Advisor** — Passive query pattern tracking with on-demand index recommendations, unused index detection, and ready-to-execute DDL

### Planned

- [ ] Real-time subscriptions
- [ ] Federated GraphQL instancing
- [ ] Embedded code execution / webhooks

---

## 🤝 Contributing

SyndrDB is approaching MVP and we'd love your help! Whether it's:
- 🐛 Reporting bugs
- 💡 Suggesting features
- 📖 Improving documentation
- 🔧 Submitting pull requests

All contributions are welcome! Check out our [contribution guidelines](https://www.syndrdb.com/contribute) to get started.

---

## 📚 Learn More

- **Full Documentation:** [www.syndrdb.com/docs](https://www.syndrdb.com/docs)
- **GraphQL Guide:** [www.syndrdb.com/docs/graphql](https://www.syndrdb. com/docs/graphql)
- **SyndrQL Reference:** [www.syndrdb.com/docs/syndrql](https://www.syndrdb.com/docs/syndrql)
- **Migration Guide:** [www.syndrdb.com/docs/migrations](https://www.syndrdb.com/docs/migrations)
- **Security Best Practices:** [www.syndrdb.com/docs/security](https://www.syndrdb. com/docs/security)

---

## 📜 License

SyndrDB Community Edition is licensed under the **Business Source License 1.1**.

### What this means:

✅ **Free for most uses:**
- Development and testing
- Production use by individuals
- Production use by companies with < 200 employees OR < $10M annual revenue
- Contributing to SyndrDB
- Academic research

❌ **Restricted uses** (require commercial license):
- Offering SyndrDB as a managed database service to third parties
- Production use by companies with ≥ $10M annual revenue
- Embedding SyndrDB in a commercial product that competes with SyndrDB

### Automatic open source conversion:
On **[4 years from v1.0 release]**, this version automatically converts to **Apache License 2.0**. 

### Commercial licensing:
Need a commercial license (Enterprise Edition)?  (COMING SOON)

---

## 💬 Community & Support

- 🌐 **Website:** [www.syndrdb. com](https://www.syndrdb.com)
- 💬 **Community:** [Join our discussions](https://www.syndrdb.com/community)
- 📧 **Email:** support@syndrdb.com
- 🐦 **Twitter:** [@SyndrDB](https://twitter.com/SyndrDB)

---

<div align="center">

### Built with ❤️ by developers, for developers

**Stop managing databases. Start building products.**

[Get Started](https://www.syndrdb.com/docs) • [Star on GitHub](https://github.com/dan-strohschein/SyndrDB) • [Follow Updates](https://www.syndrdb.com/blog)

</div>

---

## 🙏 Acknowledgments

SyndrDB stands on the shoulders of giants.  We're inspired by:
- **PostgreSQL** - Query planning and optimization techniques
- **MongoDB** - Document-oriented flexibility
- **GraphQL** - Modern API design patterns
- **The Go Community** - For an amazing ecosystem

---

<sub>Made with 🔥 in pure Go. No dependencies. No nonsense. Just powerful database technology.</sub>
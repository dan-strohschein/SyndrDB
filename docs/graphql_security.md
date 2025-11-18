Layer 1: Query Complexity Analysis (PRIMARY)
JavaScript
// syndrdb-graphql-security.js
import { createComplexityLimitRule } from 'graphql-query-complexity';

const complexityRule = createComplexityLimitRule(1000, {
  estimators: [
    // Default: 1 point per scalar field
    defaultComplexityEstimator,
    
    // Custom: Relationships multiply
    fieldExtensionsEstimator(),
    
    // SyndrDB-specific: Bundle relationships
    (args) => {
      // If field returns array, multiply by limit (or default 100)
      if (args.type.ofType?.name && args.field.name.endsWith('s')) {
        const limit = args.args?.limit || 100;
        return limit;
      }
      return 1;
    }
  ],
  
  onComplete: (complexity) => {
    console.log(`Query complexity: ${complexity}`);
  }
});
SyndrDB Schema Example:

GraphQL
type Query {
  authors(limit: Int = 10): [Author!]!
  # Complexity: limit × child fields
}

type Author {
  id: ID!
  name: String!
  books(limit: Int = 10): [Book!]!
  # Complexity: parent_limit × limit × child fields
}
Cost calculation:

GraphQL
query {
  authors(limit: 10) {     # 10 ×
    id                     #   1 = 10
    name                   #   1 = 10
    books(limit: 5) {      #   5 ×
      id                   #     1 = 50
      title                #     1 = 50
    }
  }
}

Total: 10 + 10 + 50 + 50 = 120 points ✅
Malicious query:

GraphQL
query {
  authors {                # No limit = 100 default ×
    id                     #   1 = 100
    books {                #   100 default ×
      id                   #     1 = 10,000
      reviews {            #     100 default ×
        id                 #       1 = 1,000,000
      }
    }
  }
}

Total: 1,010,100 points ❌ REJECTED
Layer 2: Depth Limiting (SECONDARY)
JavaScript
import depthLimit from 'graphql-depth-limit';

const server = new ApolloServer({
  schema,
  validationRules: [
    complexityRule,     // Primary defense
    depthLimit(7)       // Catch extremely deep nesting
  ]
});
Why 7 levels?

Allows reasonable queries: authors → books → reviews → user
Blocks cartesian products: authors → books → authors → books → ...
Layer 3: Per-User Rate Limiting (TERTIARY)
JavaScript
import rateLimit from 'graphql-rate-limit';

const rateLimiter = rateLimit({
  identifyContext: (ctx) => ctx.session?.userId || ctx.ip,
  
  // Different limits per auth state
  formatError: ({ fieldName }) => {
    return `Rate limit exceeded on field "${fieldName}"`;
  }
});

const resolvers = {
  Query: {
    authors: rateLimiter({
      max: 100,           // 100 requests
      window: '15m'       // per 15 minutes
    }),
    
    // Lower limit for expensive operations
    aggregations: rateLimiter({
      max: 10,
      window: '1h'
    })
  }
};
Layer 4: Execution Timeout (LAST RESORT)
JavaScript
const server = new ApolloServer({
  schema,
  plugins: [
    {
      requestDidStart() {
        const timeout = setTimeout(() => {
          throw new GraphQLError('Query timeout (5s)');
        }, 5000);
        
        return {
          willSendResponse() {
            clearTimeout(timeout);
          }
        }
      }
    }
  ]
});
Layer 5: Monitoring & Alerting
JavaScript
const server = new ApolloServer({
  schema,
  plugins: [
    {
      requestDidStart({ request, context }) {
        const start = Date.now();
        let complexity = 0;
        
        return {
          validationDidStart() {
            complexity = calculateComplexity(request.query);
          },
          
          willSendResponse({ errors }) {
            const duration = Date.now() - start;
            
            // Log expensive queries
            if (complexity > 500 || duration > 1000) {
              console.warn({
                user: context.userId,
                query: request.query,
                complexity,
                duration,
                errors
              });
              
              // Alert if repeated abuse
              if (complexity > 800) {
                sendAlert(`User ${context.userId} sent expensive query`);
              }
            }
          }
        }
      }
    }
  ]
});
Optional: Persisted Queries (For Paranoid Security) 🔐
If you want MAXIMUM security:

JavaScript
// Build step: Extract queries from React code
// scripts/extract-queries.js
const fs = require('fs');
const { parse } = require('graphql');

const queries = {};

// Scan all .tsx files
glob.sync('src/**/*.tsx').forEach(file => {
  const content = fs.readFileSync(file, 'utf8');
  
  // Find gql`...` or graphql`...` template literals
  const matches = content.matchAll(/(?:gql|graphql)`([^`]+)`/g);
  
  for (const match of matches) {
    const query = match[1];
    const hash = sha256(query);
    queries[hash] = query;
  }
});

// Save to registry
fs.writeFileSync('query-registry.json', JSON.stringify(queries));
Server validation:

JavaScript
const queryRegistry = require('./query-registry.json');

const server = new ApolloServer({
  schema,
  plugins: [
    {
      requestDidStart({ request }) {
        const hash = request.extensions?.persistedQuery?.sha256Hash;
        
        if (process.env.NODE_ENV === 'production' && !hash) {
          throw new Error('Only persisted queries allowed in production');
        }
        
        if (hash && !queryRegistry[hash]) {
          throw new Error('Query not registered');
        }
        
        if (hash) {
          request.query = queryRegistry[hash];
        }
      }
    }
  ]
});
React client:

TypeScript
import { gql, useQuery } from '@apollo/client';
import { createPersistedQueryLink } from '@apollo/client/link/persisted-queries';
import { sha256 } from 'crypto-hash';

const link = createPersistedQueryLink({ sha256 });

const GET_AUTHORS = gql`
  query GetAuthors {
    authors(limit: 10) {
      id
      name
    }
  }
`;

// Apollo automatically sends hash instead of full query
const { data } = useQuery(GET_AUTHORS);
Comparison: Different Approaches 📊
Approach	Security	DX	Performance	Complexity
Hasura Whitelist	🔒🔒🔒	❌	⚡⚡⚡	😰😰
Query Complexity	🔒🔒	✅✅	⚡⚡	😊
Depth Limit	🔒	✅✅	⚡⚡⚡	😊😊😊
Rate Limiting	🔒	✅✅	⚡⚡	😊😊
Persisted Queries	🔒🔒🔒	✅	⚡⚡⚡	😰😰
My Recommendation	🔒🔒🔒	✅✅	⚡⚡	😊
Real-World Example: GitHub GraphQL API 🐙
Code
Query complexity budget: 1,000 points per query
Rate limit: 5,000 points per hour
Depth limit: Not publicly stated (estimated ~10)

Example costs:
- User field: 1 point
- Repositories field: 1 + (limit × child costs)
- Comments field: 1 + (100 × child costs)  [default limit]
GitHub's response headers:

Code
X-RateLimit-Limit: 5000
X-RateLimit-Remaining: 4850
X-RateLimit-Reset: 1642520400
X-RateLimit-Used: 150
X-RateLimit-Resource: graphql
Sassy Summary 💅
Hasura's whitelist approach:

"Sure, you're secure. But now your developers hate you and deployment is a nightmare. Every new query = new whitelist entry = new deployment. Hard pass."

My recommendation:

"Query complexity + depth limit + rate limiting = the industry standard for a reason. GitHub does it. Shopify does it. Airbnb does it. It works, it scales, and developers don't want to murder you. Add persisted queries in v2 if you're feeling extra paranoid."

The magic combo:

Code
1. Complexity limit (1000 points) ← Prevents expensive queries
2. Depth limit (7 levels) ← Prevents cartesian products
3. Rate limit (100 req/15min) ← Prevents brute force
4. Timeout (5 seconds) ← Kills runaways
5. Monitoring ← Catches the 0.1% edge cases
Implementation time: 1 week

Day 1-2: Query complexity
Day 3: Depth limiting
Day 4: Rate limiting
Day 5: Monitoring/alerting
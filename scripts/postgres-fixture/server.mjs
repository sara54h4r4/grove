// Isolated PostgreSQL (PGlite, Apache-2.0), speaking Neon's SQL HTTP response
// format for integration tests. Never accepts a real connection credential.
import {PGlite} from '@electric-sql/pglite';
import {createServer} from 'node:http';

const db = new PGlite();
await db.waitReady;
const server = createServer(async (request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    response.writeHead(200);
    response.end('ready');
    return;
  }
  if (request.method !== 'POST' || request.headers['neon-connection-string'] !== 'fixture-unused') {
    response.writeHead(403);
    response.end('isolated fixture credential required');
    return;
  }
  try {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const {query, params} = JSON.parse(Buffer.concat(chunks).toString());
    const result = await db.query(query, params);
    const fields = result.fields;
    const rows = result.rows.map(row => fields.map(field => {
      const value = row[field.name];
      if (value == null) return null;
      if ([114, 3802].includes(field.dataTypeID)) return JSON.stringify(value);
      return String(value);
    }));
    response.writeHead(200, {'content-type': 'application/json'});
    response.end(JSON.stringify({fields, rows, rowCount: result.affectedRows ?? rows.length}));
  } catch (error) {
    console.error('Postgres fixture:', error.message);
    response.writeHead(400, {'content-type': 'application/json'});
    response.end(JSON.stringify({error: 'fixture SQL rejected'}));
  }
});
server.listen(Number(process.env.GROVE_TEST_SQL_PORT || 45217), '127.0.0.1', () => {
  console.log(`Grove SQL fixture ready on ${server.address().port}`);
});
process.on('SIGTERM', () => server.close(() => process.exit(0)));

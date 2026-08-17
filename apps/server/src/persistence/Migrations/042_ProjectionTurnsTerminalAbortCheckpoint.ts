import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_turns)
  `;

  if (!columns.some((column) => column.name === "is_terminal_abort_checkpoint")) {
    yield* sql`
      ALTER TABLE projection_turns
      ADD COLUMN is_terminal_abort_checkpoint INTEGER NOT NULL DEFAULT 0
    `;
  }
});

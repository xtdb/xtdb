package xtdb.antlr;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

class SqlParserTest {
    @Test
    void testSqlParser() {
        long start = System.nanoTime();
        for (int i = 0; i < 10000; i++) {
            SqlLexer lexer = new SqlLexer(CharStreams.fromString("select * from foo"));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlParser parser = new SqlParser(tokens);
            parser.queryExpression();
        }
        System.out.println("Time: " + (System.nanoTime() - start) / 1000_000 + "ms");
    }
}

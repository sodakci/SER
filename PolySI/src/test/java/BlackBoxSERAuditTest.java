import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import verifier.Pruning;
import verifier.SERVerifier;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlackBoxSERAuditTest {

    @TempDir
    Path tempDir;

    @Test
    void auditCli_acceptsSerializableHistory() throws Exception {
        var result = runAudit(List.of(
                "w(1,1,1,1)",
                "r(1,1,2,2)"
        ));

        assertEquals(0, result.exitCode);
        assertTrue(result.stderr.contains("[[[[ ACCEPT ]]]]"),
                () -> "expected ACCEPT marker, stderr was:\n" + result.stderr);
    }

    @Test
    void auditCli_rejectsNonSerializableHistory() throws Exception {
        var result = runAudit(List.of(
                "r(1,0,1,1)",
                "r(2,0,1,1)",
                "w(1,1,1,1)",
                "r(1,0,2,2)",
                "r(2,0,2,2)",
                "w(2,1,2,2)"
        ));

        assertEquals(-1, result.exitCode);
        assertTrue(result.stderr.contains("[[[[ REJECT ]]]]"),
                () -> "expected REJECT marker, stderr was:\n" + result.stderr);
        assertTrue(result.stderr.contains("[SER] Reject reason:"),
                () -> "expected rejection reason, stderr was:\n" + result.stderr);
        assertTrue(result.stdout.contains("Cycle witness:"),
                () -> "expected cycle witness, stdout was:\n" + result.stdout);
        assertTrue(result.stdout.contains("RW key="),
                () -> "expected cycle edge labels, stdout was:\n" + result.stdout);
    }

    @Test
    void auditCli_reportsPurePredicateRwCycle() throws Exception {
        var historyDir = tempDir.resolve("pure-prrw");
        Files.createDirectories(historyDir);
        Files.write(historyDir.resolve("history.prhist.jsonl"), List.of(
                "{\"session\":0,\"txn\":0,\"kind\":\"inventory.t0\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"pr\",\"predicate\":{\"kind\":\"inventory_threshold\",\"key_prefix\":\"inventory_onhand_x\",\"comparator\":\"ge\",\"threshold\":100},\"results\":["
                        + "{\"key\":\"inventory_onhand_x\",\"value\":130000001,\"semantic\":130,\"source_txn\":-1}]},"
                        + "{\"type\":\"w\",\"key\":\"inventory_onhand_z\",\"value\":400000003,\"semantic\":40}]}",
                "{\"session\":1,\"txn\":1,\"kind\":\"inventory.t1\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"pr\",\"predicate\":{\"kind\":\"inventory_threshold\",\"key_prefix\":\"inventory_onhand_y\",\"comparator\":\"ge\",\"threshold\":100},\"results\":["
                        + "{\"key\":\"inventory_onhand_y\",\"value\":130000002,\"semantic\":130,\"source_txn\":-1}]},"
                        + "{\"type\":\"w\",\"key\":\"inventory_onhand_x\",\"value\":400000001,\"semantic\":40}]}",
                "{\"session\":2,\"txn\":2,\"kind\":\"inventory.t2\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"pr\",\"predicate\":{\"kind\":\"inventory_threshold\",\"key_prefix\":\"inventory_onhand_z\",\"comparator\":\"ge\",\"threshold\":100},\"results\":["
                        + "{\"key\":\"inventory_onhand_z\",\"value\":130000003,\"semantic\":130,\"source_txn\":-1}]},"
                        + "{\"type\":\"w\",\"key\":\"inventory_onhand_y\",\"value\":400000002,\"semantic\":40}]}"));

        var result = runAuditCommand("audit", "-t", "PRHIST", historyDir.toString());

        assertEquals(-1, result.exitCode);
        assertTrue(result.stdout.contains("Cycle witness:"),
                () -> "expected cycle witness, stdout was:\n" + result.stdout);
        assertTrue(result.stdout.contains("PR_RW key=inventory_onhand_x"),
                () -> "expected predicate RW edge for x, stdout was:\n" + result.stdout);
        assertTrue(result.stdout.contains("PR_RW key=inventory_onhand_y"),
                () -> "expected predicate RW edge for y, stdout was:\n" + result.stdout);
        assertTrue(result.stdout.contains("PR_RW key=inventory_onhand_z"),
                () -> "expected predicate RW edge for z, stdout was:\n" + result.stdout);
    }

    private CliResult runAudit(List<String> historyLines) throws Exception {
        var historyFile = tempDir.resolve("history.txt");
        Files.write(historyFile, historyLines);

        return runAuditCommand("audit", "-t", "text", historyFile.toString());
    }

    private CliResult runAuditCommand(String... args) throws Exception {
        Pruning.setEnablePruning(true);
        SERVerifier.setCoalesceConstraints(true);
        SERVerifier.setDotOutput(false);
        SERVerifier.setCompareLegacyPredicateEdges(false);

        var stdout = new ByteArrayOutputStream();
        var stderr = new ByteArrayOutputStream();
        var oldOut = System.out;
        var oldErr = System.err;

        try {
            System.setOut(new PrintStream(stdout, true));
            System.setErr(new PrintStream(stderr, true));

            var cmd = new CommandLine(new Main());
            cmd.setCaseInsensitiveEnumValuesAllowed(true);
            int exitCode = cmd.execute(args);
            return new CliResult(exitCode, stdout.toString(), stderr.toString());
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
        }
    }

    private static class CliResult {
        private final int exitCode;
        private final String stdout;
        private final String stderr;

        private CliResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }
}

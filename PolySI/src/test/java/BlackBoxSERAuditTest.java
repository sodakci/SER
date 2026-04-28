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
    }

    private CliResult runAudit(List<String> historyLines) throws Exception {
        var historyFile = tempDir.resolve("history.txt");
        Files.write(historyFile, historyLines);

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
            int exitCode = cmd.execute("audit", "-t", "text", historyFile.toString());
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

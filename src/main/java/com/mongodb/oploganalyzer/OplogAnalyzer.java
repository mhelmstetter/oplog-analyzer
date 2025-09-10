package com.mongodb.oploganalyzer;

import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(name = "oplog-analyzer", mixinStandardHelpOptions = true, version = "0.0.1-SNAPSHOT",
         description = "MongoDB Oplog Analyzer",
         subcommands = {ScanCommand.class, TailCommand.class, SampleCommand.class, AnalyzeCommand.class})
public class OplogAnalyzer implements Callable<Integer> {
	
	// Main command - no fields needed since all functionality is in subcommands
    
    @Spec 
    CommandSpec spec;

    @Override
    public Integer call() {
        // Get available subcommands dynamically from the CommandSpec
        String subcommands = spec.subcommands().keySet().stream()
            .sorted()
            .collect(Collectors.joining(", "));
        
        System.err.println("Please specify a subcommand: " + subcommands);
        System.err.println("Use --help to see available commands and options");
        return 1;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new OplogAnalyzer()).execute(args);
        System.exit(exitCode);
    }


}

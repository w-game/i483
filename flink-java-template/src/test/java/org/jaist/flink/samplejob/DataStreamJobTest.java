/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit5TestClass.java to edit this template
 */
package org.jaist.flink.samplejob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Disabled;

/**
 *
 * @author haha
 */
public class DataStreamJobTest {
    
    public DataStreamJobTest() {
    }
    
    @BeforeAll
    public static void setUpClass() {
    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() {
    }
    
    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of main method, of class DataStreamJob.
     * @throws java.lang.Exception
     */
    @Disabled
    @Test
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testConstructor() throws Exception {
        System.out.println("constructor");
        // new DataStreamJob(StreamExecutionEnvironment.createLocalEnvironment());
        DataStreamJob.buildPipeline(StreamExecutionEnvironment.createLocalEnvironment());
    }
    
}

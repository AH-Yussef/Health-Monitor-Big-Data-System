package com.epokh.hdfs;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Endpoints {
    private RangeQueryHandler handler;
    public Endpoints() throws IOException {
        handler = new RangeQueryHandler();
    }

    @GetMapping("/getAnalytics")
    @CrossOrigin
    @ResponseBody
    public Map<String, String> getAnalytics(@RequestParam String start, @RequestParam String end) throws ClassNotFoundException, IOException, SQLException, ParseException, InterruptedException, ExecutionException {
        // long start = System.nanoTime();
        // long end = System.nanoTime();

        // System.out.println("Time");
        // System.out.println((end-start)*Math.pow(10,-6));
        return handler.getInfo(start, end);
    }
}
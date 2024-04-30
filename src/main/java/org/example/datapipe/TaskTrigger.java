package org.example.datapipe;

import Task.TimedTask;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;


import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@WebServlet(loadOnStartup = 2, urlPatterns = "/trigger")
public class TaskTrigger extends HttpServlet {

    private static final long ONE_MONTH_IN_MILLIS = 2629800000L; // 30.5 days in milliseconds
    private static final String CATEGORY1 = "ELECTRICITY";
    private static final String CATEGORY2 = "WATER";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Override
    public void init() throws ServletException {
        super.init();
        scheduleMonthlyUpdate();
    }

    private void scheduleMonthlyUpdate() {
        TimedTask timedTask = new TimedTask();
        Runnable task = () -> timedTask.updateAccountBalances(CATEGORY1);
        Runnable task2 = () -> timedTask.updateAccountBalances(CATEGORY2);
        scheduler.scheduleAtFixedRate(task, ONE_MONTH_IN_MILLIS, ONE_MONTH_IN_MILLIS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(task2, ONE_MONTH_IN_MILLIS, ONE_MONTH_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("TaskTrigger is running");
    }

    @Override
    public void destroy() {
        scheduler.shutdown();
        super.destroy();
    }
}

package com.clouway.testapp;

import com.google.appengine.api.datastore.*;
import com.google.appengine.repackaged.com.google.common.io.ByteStreams;
import com.google.common.collect.Sets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

/**
 * @author Miroslav Genov (mgenov@gmail.com)
 */
public class BenchmarkDataImportServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse resp) throws ServletException, IOException {

        String task = request.getParameter("task");

        DatastoreService dss = DatastoreServiceFactory.getDatastoreService();

        PrintWriter writer = resp.getWriter();

        long start = System.currentTimeMillis();

        if ("add_entity_with_list".equals(task)) {
            byte[] content = ByteStreams.toByteArray(BenchmarkDataImportServlet.class.getResourceAsStream("test.json"));
            Entity entity = new Entity("Index");
            entity.setProperty("content", new Blob(content));
            Set<String> index = Sets.newHashSet();
            for (int i = 0;i < 500;i++) {
                index.add("index_" + i);
            }
            entity.setProperty("index", index);
            entity.setProperty("index_2", index);

            dss.put(entity);
        }

        if ("add_entity".equals(task)) {
            byte[] content = ByteStreams.toByteArray(BenchmarkDataImportServlet.class.getResourceAsStream("test.json"));
            Entity entity = new Entity("Index");
            entity.setProperty("content", new Blob(content));
            dss.put(entity);
        }


        if ("load_entity".equals(task)) {
            Key key = KeyFactory.createKey("Index",Long.valueOf(request.getParameter("id")));
            try {
                Entity entity = dss.get(key);
            } catch (EntityNotFoundException e) {
                e.printStackTrace();
            }
        }

        if ("query".equals(task)) {
            for (Entity entity : dss.prepare(new Query("Index")).asIterable()) {
                Blob content = (Blob) entity.getProperty("content");
                String json  = new String(content.getBytes(),"UTF-8");
                System.out.println("...");
            }
        }

        long end = System.currentTimeMillis();

        writer.println("Execution time: " + (end - start) + " ms");

        writer.flush();
        writer.close();
    }
}

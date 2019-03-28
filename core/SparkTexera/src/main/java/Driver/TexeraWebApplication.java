package Driver;

import io.dropwizard.Application;
import io.dropwizard.jersey.sessions.HttpSessionFactory;
import io.dropwizard.setup.Environment;
import io.dropwizard.jersey.sessions.SessionFactoryProvider;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.nio.file.Paths;
import java.util.EnumSet;

public class TexeraWebApplication extends Application<TexeraWebConfiguration> {

    public static void main(String[] args) throws Exception {
        new TexeraWebApplication().run(args);
    }


    @Override
    public void run(TexeraWebConfiguration texeraWebConfiguration, Environment environment) throws Exception {
        // serve backend at /api
        environment.jersey().setUrlPattern("/api/*");

        final QueryPlanResource newQueryPlanResource = new QueryPlanResource();
        environment.jersey().register(newQueryPlanResource);

        final SystemResource systemResource = new SystemResource();
        environment.jersey().register(systemResource);


        // Enable CORS headers
        final FilterRegistration.Dynamic cors =
                environment.servlets().addFilter("CORS", CrossOriginFilter.class);
        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");
        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

        environment.servlets().setSessionHandler(new SessionHandler());
        environment.jersey().register(SessionFactoryProvider.class);

    }
}

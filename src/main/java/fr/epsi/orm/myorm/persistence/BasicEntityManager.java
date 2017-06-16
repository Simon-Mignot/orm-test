package fr.epsi.orm.myorm.persistence;

import fr.epsi.orm.myorm.annotation.Entity;
import fr.epsi.orm.myorm.annotation.Id;
import fr.epsi.orm.myorm.annotation.Transient;
import fr.epsi.orm.myorm.lib.NamedPreparedStatement;
import fr.epsi.orm.myorm.lib.ReflectionUtil;
import javaslang.Predicates;
import sun.reflect.Reflection;

import javax.sql.DataSource;
import javax.swing.text.html.Option;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static fr.epsi.orm.myorm.persistence.MappingHelper.*;
import static fr.epsi.orm.myorm.persistence.SqlGenerator.*;

/**
 * Created by fteychene on 14/05/17.
 */
public class BasicEntityManager implements EntityManager
{

    private final DataSource datasource;
    private final Set<Class<?>> persistentClasses;


    private BasicEntityManager(DataSource aDataSource, Set<Class<?>> aPersistentClasses)
    {
        datasource = aDataSource;
        persistentClasses = aPersistentClasses;
    }

    /**
     * Check the Persistent classes to be managed by the EntityManager to have the minimal configuration.
     * <p>
     * Each class should respect the following rules :
     * - Class should be annotated with @Entity
     * - Class should have one and only one field with the @Id annotation
     *
     * @param persistentClasses
     * @throws IllegalArgumentException if a class does not match the conditions
     */
    private static void checkPersistentClasses(Set<Class<?>> persistentClasses)
    {

    }

    /**
     * Check id a Class is managed by this EntityManager
     *
     * @param checkClass
     */
    private void isManagedClass(Class<?> checkClass)
    {
        if(!persistentClasses.contains(checkClass))
        {
            throw new IllegalArgumentException("The class " + checkClass.getName() + " is not managed by this EntityManager ...");
        }
    }

    /**
     * Create a BasicEntityManager and check the persistents classes
     *
     * @param dataSource        The Datasource to use for connecting to DB
     * @param persistentClasses The Set of Classes to be managed in this EntityManager
     * @return The BasicEntityManager created
     */
    public static BasicEntityManager create(DataSource dataSource, Set<Class<?>> persistentClasses)
    {
        checkPersistentClasses(persistentClasses);
        return new BasicEntityManager(dataSource, persistentClasses);
    }

    /**
     * @see EntityManager#find(Class, Object)
     */
    @Override
    public <T> Optional<T> find(Class<T> entityClass, Object id)
    {
        isManagedClass(entityClass);
        return ReflectionUtil.getFieldDeclaringAnnotation(entityClass, Id.class)
                .map((idField) ->
                {
                    List<T> result = executeQuery(entityClass, SqlGenerator.generateSelectSql(entityClass, idField), new HashMap<String, Object>()
                    {{
                        put("id", id);
                    }});
                    return result.isEmpty() ? null : result.get(0);
                });
    }

    /**
     * @see EntityManager#findAll(Class)
     */
    @Override
    public <T> List<T> findAll(Class<T> entityClass)
    {
        isManagedClass(entityClass);

        return executeQuery(entityClass, SqlGenerator.generateSelectSql(entityClass), new HashMap<>());
    }

    /**
     * @see EntityManager#save(Object)
     */
    @Override
    public <T> Optional<T> save(T entity)
    {
        isManagedClass(entity.getClass());
        String s = SqlGenerator.generateInsertInto(entity.getClass(), entity);
        System.out.println(s);
        try
        {
            executeUpdate(s, new HashMap<String, Object>());
            return Optional.of(entity);
        } catch(SQLException e)
        {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * @see EntityManager#delete(Object)
     */
    @Override
    public <T> boolean delete(T entity)
    {
        isManagedClass(entity.getClass());
        try
        {
            Field idField = ReflectionUtil.getFieldDeclaringAnnotation(entity.getClass(), Id.class).get();
            String sql = SqlGenerator.generateDeleteSql(entity.getClass());
            int affectedRows = executeUpdate(sql, new HashMap<String, Object>()
            {{
                put(idField.getName(), ReflectionUtil.getValue(idField, entity).get());
            }});
            return affectedRows > 0;
        } catch(SQLException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    private <T> List<T> executeQuery(Class<T> entityClass, String sql, Map<String, Object> parameters)
    {
        try
        {
            NamedPreparedStatement statement = NamedPreparedStatement.prepare(datasource.getConnection(), sql);
            statement.setParameters(parameters);
            ResultSet resultSet = statement.executeQuery();
            return MappingHelper.mapFromResultSet(entityClass, resultSet);
        }
        catch(SQLException e)
        {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    private <T> int executeUpdate(String sql, Map<String, Object> parameters) throws SQLException
    {
        System.out.println(sql);
        NamedPreparedStatement statement = NamedPreparedStatement.prepare(datasource.getConnection(), sql);
        statement.setParameters(parameters);
        return statement.executeUpdate();
    }
}

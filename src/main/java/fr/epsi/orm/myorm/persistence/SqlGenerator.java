package fr.epsi.orm.myorm.persistence;

import fr.epsi.orm.myorm.annotation.Column;
import fr.epsi.orm.myorm.annotation.Entity;
import fr.epsi.orm.myorm.annotation.Id;
import fr.epsi.orm.myorm.lib.ReflectionUtil;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import sun.reflect.Reflection;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by fteychene on 14/05/17.
 */
public class SqlGenerator
{

    public static String getColumnNameForField(Field field)
    {
        return ReflectionUtil.getAnnotationForField(field, Column.class)
                .map((column) ->
                {
                    if(column.name().equals(""))
                    {
                        return null;
                    }
                    return column.name();
                }).orElse(field.getName());
    }


    public static String getTableForEntity(Class<?> clazz)
    {

        return ReflectionUtil.getAnnotationForClass(clazz, Entity.class)
                .map((entity) ->
                {
                    if(entity.table().equals(""))
                    {
                        return null;
                    }
                    return entity.table();
                }).orElse(clazz.getSimpleName());
    }

    public static String generateDeleteSql(Class<?> entityClass)
    {
        return ReflectionUtil.getFieldDeclaringAnnotation(entityClass, Id.class)
                .map((f) ->
                        "DELETE FROM " +
                                getTableForEntity(entityClass) +
                                " WHERE " + getColumnNameForField(f) + " = :" + f.getName())
                .orElse("");
    }

    public static String generateSelectForEntity(Class<?> clazz)
    {
        return ReflectionUtil.getFieldsWithoutTransient(clazz)
                .map((f) -> getColumnNameForField(f).concat(" as ").concat(f.getName()))
                .collect(Collectors.joining(", "));
    }

    public static <T> String generateInsertIntoColumns(Class<?> entityClass, T entity)
    {
        /*return ReflectionUtil.getFieldsWithoutTransient(entityClass)
                .map((f) -> getColumnNameForField(f))
                .collect(Collectors.joining(", "));*/
        System.out.println("HERE2");
        return ReflectionUtil.getFieldsWithoutTransient(entityClass)
                .map((f) ->
                {
                    Optional<T> o = ReflectionUtil.getValue(f, entity);
                    Object v = o.orElse(null);
                    String column = getColumnNameForField(f);
                    if(column.equals("id"))
                        column = null;
                    return v != null ? column : null; // injection
                })
                .filter((s) -> s != null && !s.isEmpty())
                .collect(Collectors.joining(", "));
    }

    public static <T> String generateInsertInto(Class<?> entityClass, T entity)
    {
        return "INSERT INTO " + getTableForEntity(entityClass) + " (" + generateInsertIntoColumns(entityClass, entity) + ")"
                + " VALUES (" + generateInsertIntoValues(entityClass, entity) + ")";
    }

    public static <T> String generateInsertIntoValues(Class<?> entityClass, T entity)
    {
        System.out.println("HERE");
        return ReflectionUtil.getFieldsWithoutTransient(entityClass)
                .map((f) ->
                {
                    Optional<T> o = ReflectionUtil.getValue(f, entity);
                    Object v = o.orElse(null);
                    String value = v != null ? "'" + v.toString() + "'" : null;
                    if(getColumnNameForField(f).equals("id"))
                        value = null;
                    if(v != null && Objects.equals(f.getGenericType().getTypeName(), "java.time.LocalDate"))
                        value = "DATE " + value;
                    System.out.println(f.getGenericType().getTypeName());
                    return value; // injection
                })
                .filter((s) -> s != null && !s.isEmpty())
                .collect(Collectors.joining(", "));
    }

    public static String generateSelectSql(Class<?> entityClass, Field... fields)
    {
        String result = "SELECT " + generateSelectForEntity(entityClass) + " FROM " + getTableForEntity(entityClass);
        if(fields.length > 0)
        {
            result = Arrays.stream(fields)
                    .map(f -> " " + getColumnNameForField(f) + " = :" + f.getName())
                    .reduce(result.concat(" WHERE"), (acc, value) -> acc.concat(value));
        }
        return result;
    }

}

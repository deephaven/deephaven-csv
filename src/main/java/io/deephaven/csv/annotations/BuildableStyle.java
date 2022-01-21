package io.deephaven.csv.annotations;

import org.immutables.value.Value;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A simple style is for objects that are simple to build. Not recommended for objects with more than two fields. Not
 * applicable for objects with default fields.
 */
@Target({ElementType.TYPE, ElementType.PACKAGE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE,
        defaults = @Value.Immutable(copy = false), strictBuilder = false, weakInterning = true,
        jdkOnly = true)
public @interface BuildableStyle {
    // Note: this produces ImmutableX.builder()s for the implementation classes
}

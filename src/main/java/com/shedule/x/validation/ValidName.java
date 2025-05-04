package com.shedule.x.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Custom annotation to validate names.
 * <p>
 * This annotation is used to validate that a name follows a specific format.
 * The format allows letters (including accented characters), apostrophes, hyphens, and spaces.
 * </p>
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = NameValidator.class)
public @interface ValidName {

    /**
     * The default error message to be used when the validation fails.
     *
     * @return the error message
     */
    String message() default "Invalid Name Format.";

    /**
     * Groups can be used to categorize constraints, allowing different validation rules to apply in different contexts.
     *
     * @return the validation groups
     */
    Class<?>[] groups() default {};

    /**
     * Payload can be used to attach additional metadata to the constraint.
     *
     * @return the payload
     */
    Class<? extends Payload>[] payload() default {};
}

package com.shedule.x.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Custom annotation to validate password strength.
 * <p>
 * This annotation ensures that the password meets the following criteria:
 * <ul>
 *     <li>At least 9 characters long.</li>
 *     <li>Contains at least one special character.</li>
 *     <li>Contains at least one uppercase letter.</li>
 *     <li>Contains at least one number.</li>
 * </ul>
 * </p>
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = PasswordValidator.class)
public @interface ValidPassword {

    /**
     * The default error message to be shown when the validation fails.
     *
     * @return the error message
     */
    String message() default "Invalid Password. Password must contain at least 9 characters, including at least one special character, one capital letter, and one number.";

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

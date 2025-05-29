package com.shedule.x.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = EnumValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidEnum {
    Class<? extends Enum<?>> enumClass();
    String message() default "Value is not valid enum";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}

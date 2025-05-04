package com.shedule.x.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class PasswordValidator implements ConstraintValidator<ValidPassword, String> {

    @Override
    public void initialize(ValidPassword constraintAnnotation) {

    }

    @Override
    public boolean isValid(String password, ConstraintValidatorContext context) {
        if (password == null || password.isEmpty()) {
            return false;
        }

        // Password must meet the following criteria:
        // - At least one uppercase letter
        // - At least one special character
        // - At least one digit
        return password.matches("^(?!.*\\s)(?=.*[A-Z])(?=.*[!@#$%^&*(),.?\":{}|<>])(?=.*\\d).+$");
    }
}


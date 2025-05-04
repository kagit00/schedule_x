package com.shedule.x.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.commons.lang3.StringUtils;

/**
 * Custom validator for validating names.
 * <p>
 * This validator checks if a name is not empty and matches a regular expression that allows
 * letters (including accented characters), apostrophes, hyphens, and spaces.
 * </p>
 */
public class NameValidator implements ConstraintValidator<ValidName, String> {

    /**
     * Validates the given name based on specified rules.
     * <p>
     * The name must not be empty and should match the regular expression that permits letters
     * (including accented letters), apostrophes, hyphens, and spaces.
     * </p>
     *
     * @param value the name string to validate
     * @param context the context in which the constraint is evaluated
     * @return true if the name is valid, false otherwise
     */
    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        return !StringUtils.isEmpty(value) && value.matches("^[A-Za-zÀ-ÖØ-öø-ÿ'’\\- ]+$");
    }
}


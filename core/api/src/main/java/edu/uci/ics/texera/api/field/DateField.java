package edu.uci.ics.texera.api.field;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;

public class DateField implements IField {

    private Date value;

    //TODO: current json serialization converts DateField to an int, which is not user friendly
    @JsonCreator
    public DateField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE, required = true)
            Date value) {
        checkNotNull(value);
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DateField other = (DateField) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DateField [value=" + value + "]";
    }

}

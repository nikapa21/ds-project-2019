package com.itshareplus.googlemapdemo;

import com.itshareplus.googlemapdemo.models.Value;
import java.io.Serializable;

public class Data implements Serializable {

    ArtistName artistName;
    Value value;

    public Data(ArtistName artistName, Value value) {

        this.artistName = artistName;
        this.value = value;
    }

    public ArtistName getTopic() {

        return artistName;
    }

    public void setTopic(ArtistName artistName) {

        this.artistName = artistName;
    }

    public Value getValue() {

        return value;
    }

    public void setValue(Value value) {

        this.value = value;
    }

    @Override
    public String toString() {

        return "Data{" +
                "artistName=" + artistName +
                ", value=" + value +
                '}';
    }
}

package com.itshareplus.googlemapdemo;

import java.io.Serializable;
import java.util.Objects;

public class ArtistName implements Serializable {

    private String artistName;

    public ArtistName(String artistName) {

        this.artistName = artistName;
    }

    public String getArtistName() {

        return artistName;
    }

    public void setArtistName(String artistName) {

        this.artistName = artistName;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArtistName topic = (ArtistName) o;
        return artistName.equals(topic.artistName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(artistName);
    }

    @Override
    public String toString() {

        return "ArtistName{" +
                "artistName='" + artistName + '\'' +
                '}';
    }
}

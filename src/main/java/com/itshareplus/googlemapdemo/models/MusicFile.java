package com.itshareplus.googlemapdemo.models;

import java.util.Arrays;

public class MusicFile {

    private String trackName;
    private String artistName;
    private String albumInfo;
    private String genre;
    private byte[] musicFileExtract;

    public MusicFile(String trackName, String artistName, String albumInfo, String genre, byte[] musicFileExtract) {

        this.trackName = trackName;
        this.artistName = artistName;
        this.albumInfo = albumInfo;
        this.genre = genre;
        this.musicFileExtract = musicFileExtract;
    }

    public String getTrackName() {

        return trackName;
    }

    public void setTrackName(String trackName) {

        this.trackName = trackName;
    }

    public String getArtistName() {

        return artistName;
    }

    public void setArtistName(String artistName) {

        this.artistName = artistName;
    }

    public String getAlbumInfo() {

        return albumInfo;
    }

    public void setAlbumInfo(String albumInfo) {

        this.albumInfo = albumInfo;
    }

    public String getGenre() {

        return genre;
    }

    public void setGenre(String genre) {

        this.genre = genre;
    }

    public byte[] getMusicFileExtract() {

        return musicFileExtract;
    }

    public void setMusicFileExtract(byte[] musicFileExtract) {

        this.musicFileExtract = musicFileExtract;
    }

    @Override
    public String toString() {

        return "MusicFile{" +
                "trackName='" + trackName + '\'' +
                ", artistName='" + artistName + '\'' +
                ", albumInfo='" + albumInfo + '\'' +
                ", genre='" + genre + '\'' +
                ", musicFileExtract=" + Arrays.toString(musicFileExtract) +
                '}';
    }
}
#!/bin/sh
start cmd /k java Master.MasterNode
start cmd /k java Chord.Node 1
start cmd /k java Chord.Node 8
start cmd /k java Chord.Node 14
start cmd /k java Chord.Node 21
start cmd /k java Chord.Node 32
start cmd /k java Chord.Node 38
start cmd /k java Chord.Node 47
start cmd /k java Chord.Node 48
start cmd /k java Chord.Node 51
start cmd /k java Chord.Node 56
start cmd /k java Test.Menu
REM start cmd /k java usernode.MobileService
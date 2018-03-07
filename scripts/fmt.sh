#!/bin/sh

sbt ';scalafmt;sbt:scalafmt;test:scalafmt'
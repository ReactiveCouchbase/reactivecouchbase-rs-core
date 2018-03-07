#!/bin/sh

sbt ";reload;clean;+compile;test;+publish;+publish-local"
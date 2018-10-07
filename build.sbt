scalaVersion := "2.11.12"

// Set to false or remove if you want to show stubs as linking errors
nativeLinkStubs := true

enablePlugins(ScalaNativePlugin)

scalafmtOnCompile in ThisBuild := true

nativeGC := "immix"

nativeMode := "debug"

// nativeLTO := "thin"

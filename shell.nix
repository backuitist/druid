{ pkgs ? import <nixpkgs> {} }:

let
  # The Python environment with pyyaml included
  pythonWithPackages = pkgs.python3.withPackages(ps: with ps; [
    pyyaml
  ]);

  makeDist = pkgs.writeShellScriptBin "druid.makeDist" ''
    ${pkgs.maven}/bin/mvn install -Dcheckstyle.skip=true -DskipTests -Pdist $@
  '';
in

pkgs.mkShell {
  # This makes the python interpreter with the packages available in your PATH
  buildInputs = [
    makeDist
    pkgs.maven
    pythonWithPackages
  ];

  shellHook = ''
    >&2 echo "Welcome to the Druid shell, here are some useful commands:"
    >&2 echo "* druid.makeDist - make a distribution"
  '';
}
{ pkgs ? import <nixpkgs> {} }:

let
  PROJECT_ROOT = builtins.toString ./.;

  # The Python environment with pyyaml included
  pythonWithPackages = pkgs.python3.withPackages(ps: with ps; [
    pyyaml
  ]);

  makeDist = pkgs.writeShellScriptBin "druid.makeDist" ''
    ${pkgs.maven}/bin/mvn install -Dcheckstyle.skip=true -DskipTests -Pdist $@
  '';

  publishDist = pkgs.writeShellScriptBin "druid.publishDist" ''
    if [[ $# -ne 1 ]]; then
      echo "USAGE: $0 <version>"
      exit 1
    fi
    VERSION=$1
    DEST=s3://third-party-pkgs.eu-west-1.hypervolt/druid/distrib/apache-druid-32.0.1-pulsar-$VERSION-bin.tar.gz
    echo "Publishing version $VERSION: $DEST"
    ${pkgs.awscli2}/bin/aws s3 cp ${PROJECT_ROOT}/distribution/target/apache-druid-32.0.1-bin.tar.gz $DEST
  '';
in

pkgs.mkShell {
  # This makes the python interpreter with the packages available in your PATH
  buildInputs = [
    makeDist
    publishDist
    pkgs.maven
    pythonWithPackages
  ];

  shellHook = ''
    >&2 echo "Welcome to the Druid shell, here are some useful commands:"
    >&2 echo "* druid.makeDist - make a distribution"
    >&2 echo "* druid.publishDist - publish a distribution"
  '';
}
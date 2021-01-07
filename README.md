# Alfa Operatr
[![Docker Pulls](https://img.shields.io/docker/pulls/illallangi/alfawtch.svg)](https://hub.docker.com/r/illallangi/alfaoperatr)
[![Image Size](https://images.microbadger.com/badges/image/illallangi/alfaoperatr.svg)](https://microbadger.com/images/illallangi/alfaoperatr)
![Build](https://github.com/illallangi/AlfaOperatr/workflows/Build/badge.svg)

Creates Kubernetes resources by processing custom resources through a Jinja2 template.

## Installation

    kubectl apply -f https://raw.githubusercontent.com/illallangi/AlfaOperatr/master/deploy.yaml

## Usage

```shell
$ alfaoperatr
Usage: alfaoperatr [OPTIONS] PARENT

Options:
  --log-level [CRITICAL|ERROR|WARNING|INFO|DEBUG|SUCCESS|TRACE]
  --slack-username TEXT
  --slack-webhook TEXT
  --slack-format TEXT
  --debug-path DIRECTORY
  --api-proxy TEXT
  --help                          Show this message and exit.
```

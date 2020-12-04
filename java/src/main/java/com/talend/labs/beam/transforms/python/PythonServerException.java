package com.talend.labs.beam.transforms.python;

public class PythonServerException extends Exception {
  private String message;

  public PythonServerException(String message) {
    this.message = message;
  }
}

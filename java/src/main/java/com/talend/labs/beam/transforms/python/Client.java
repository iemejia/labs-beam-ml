package com.talend.labs.beam.transforms.python;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class Client {

  private String host;
  private int port;

  private Socket socket;
  private DataOutputStream dataOutputStream;

  Client(String host, int port) throws IOException {
    this.socket = new Socket(host, port);
    this.dataOutputStream = new DataOutputStream(this.socket.getOutputStream());
  }

  /**
   * @param code python code
   * @return id of the function in the server
   */
  String registerCode(String code) {
    byte[] action = {0x00};
    byte[] payload = code.getBytes(StandardCharsets.UTF_8);
    byte[] payloadLength = ByteBuffer.allocate(4).putInt(payload.length).array();

    byte[] data = new byte[action.length + payload.length + payloadLength.length];
    System.arraycopy(action, 0, data, 0, action.length);
    System.arraycopy(payloadLength, 0, data, action.length, payloadLength.length);
    System.arraycopy(payload, 0, data, action.length + payloadLength.length, payload.length);

    String response = request(data);
    return response;
  }

  /**
   * @param codeId id of the function in the server
   * @param element to process
   * @return processed element
   */
  String execute(String codeId, String element) {
    byte[] action = {0x01};
    byte[] codeIdBytes = codeId.getBytes(StandardCharsets.UTF_8);
    byte[] payload = element.getBytes(StandardCharsets.UTF_8);

    byte[] data = new byte[action.length + payload.length + codeIdBytes.length];
    System.arraycopy(action, 0, data, 0, action.length);
    System.arraycopy(codeIdBytes, 0, data, action.length, codeIdBytes.length);
    System.arraycopy(payload, 0, data, action.length + codeIdBytes.length, payload.length);

    String response = request(data);
    return response;
  }

  private String request(byte[] data) {
    try {
      dataOutputStream.write(data);
      dataOutputStream.flush();

      InputStream in = socket.getInputStream();
      StringBuilder result = new StringBuilder();
      do {
        result.append((char) in.read());
      } while (in.available() > 0);
      String output = result.toString();
      return output;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void close() {
    try {
      if (dataOutputStream != null) {
        dataOutputStream.flush();
        dataOutputStream.close();
        dataOutputStream = null;
      }
      if (socket != null) {
        socket.close();
        socket = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
      // ?
    }
  }
}

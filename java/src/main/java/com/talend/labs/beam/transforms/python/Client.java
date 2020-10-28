package com.talend.labs.beam.transforms.python;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.primitives.Bytes;

class Client {

  private String host;
  private int port;

  private Socket socket;
  private DataOutputStream dataOutputStream;
  private final DataInputStream dataInputStream;

  Client(String host, int port) throws IOException {
    this.socket = new Socket(host, port);
    this.dataOutputStream = new DataOutputStream(this.socket.getOutputStream());
    this.dataInputStream = new DataInputStream(socket.getInputStream());
  }

  /**
   * @param code python code
   * @return id of the function in the server
   */
  String registerCode(String code) {
    byte[] data = Bytes.concat(new byte[]{0x00}, lengthPrefixedBytes(code));
//    String response = request(data);
    return requestOne(data);
  }

  /**
   * @param codeId id of the function in the server
   * @param element to process
   * @return processed element
   */
  List<String> execute(String codeId, String element) {
    byte[] data =
        Bytes.concat(new byte[] {0x01}, lengthPrefixedBytes(codeId), lengthPrefixedBytes(element));
//    String response = request(data);
//    return response;
    return request(data);
  }

  private static byte[] lengthPrefixedBytes(String value) {
    return lengthPrefixedBytes(value.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] lengthPrefixedBytes(byte[] bytes) {
    return Bytes.concat(ByteBuffer.allocate(4).putInt(bytes.length).array(), bytes);
  }

  private String requestOne(byte[] req) {
    try {
      dataOutputStream.write(req);
      dataOutputStream.flush();
      int length = dataInputStream.readInt();
      byte[] data = new byte[length];
      int read = dataInputStream.read(data);
      return new String(data, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  private List<String> request(byte[] req) {
    try {
      dataOutputStream.write(req);
      dataOutputStream.flush();

      List<String> results = new ArrayList<>();
      int length = -1;
      do {
        length = dataInputStream.readInt();
        if (length == -1) {
          break;
        }
        byte[] data = new byte[length];
        int read = dataInputStream.read(data);
        results.add(new String(data, StandardCharsets.UTF_8));
      } while (length != -1);
      return results;
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

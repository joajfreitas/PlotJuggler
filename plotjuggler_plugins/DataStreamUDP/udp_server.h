/*DataStreamServer PlotJuggler  Plugin license(Faircode)

Copyright(C) 2018 Philippe Gauthier - ISIR - UPMC
Permission is hereby granted to any person obtaining a copy of this software and
associated documentation files(the "Software"), to deal in the Software without
restriction, including without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and / or sell copies("Use") of the Software, and to permit persons
to whom the Software is furnished to do so. The above copyright notice and this permission
notice shall be included in all copies or substantial portions of the Software. THE
SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
#pragma once

#include <QUdpSocket>
#include <QtPlugin>
#include <QThread>
#include <QNetworkDatagram>
#include <QMessageBox>
#include <thread>
#include <mutex>
#include "PlotJuggler/datastreamer_base.h"
#include "PlotJuggler/messageparser_base.h"

using namespace PJ;

class Worker: public QObject {
  Q_OBJECT

  public:
  Worker() = default;
  Worker(QUdpSocket* udp_socket, PJ::MessageParserPtr parser): _udp_socket{udp_socket}, _parser{parser} {};

  void process() {
    while (_udp_socket->hasPendingDatagrams())
    {
      QNetworkDatagram datagram = _udp_socket->receiveDatagram();

      using namespace std::chrono;
      auto ts = high_resolution_clock::now().time_since_epoch();
      double timestamp = 1e-6 * double(duration_cast<microseconds>(ts).count());

      QByteArray m = datagram.data();
      MessageRef msg(reinterpret_cast<uint8_t*>(m.data()), m.count());

      try
      {
        std::mutex m{};
        std::lock_guard<std::mutex> lock(m);
        // important use the mutex to protect any access to the data
        _parser->parseMessage(msg, timestamp);
      }
      catch (std::exception& err)
      {
        QMessageBox::warning(nullptr, tr("UDP Server"),
                             tr("Problem parsing the message. UDP Server will be "
                                "stopped.\n%1")
                                 .arg(err.what()),
                             QMessageBox::Ok);
        //shutdown();
        //// notify the GUI
        emit closed();
        return;
      }
    }
    //// notify the GUI
    emit dataReceived();
    std::cout << "out" << std::endl;
    return;
  }

  private:
  QUdpSocket* _udp_socket;
  PJ::MessageParserPtr _parser;

  signals:
    void dataReceived(void);
    void closed(void);
    void shutdown(void);

};

class UDP_Server : public PJ::DataStreamer
{
  Q_OBJECT
  Q_PLUGIN_METADATA(IID "facontidavide.PlotJuggler3.DataStreamer")
  Q_INTERFACES(PJ::DataStreamer)

public:
  UDP_Server();

  virtual bool start(QStringList*) override;

  virtual void shutdown() override;

  virtual bool isRunning() const override
  {
    return _running;
  }

  virtual ~UDP_Server() override;

  virtual const char* name() const override
  {
    return "UDP Server";
  }

  virtual bool isDebugPlugin() override
  {
    return false;
  }

private:
  bool _running;
  QUdpSocket* _udp_socket;
  PJ::MessageParserPtr _parser;
  QThread _worker_thread;
  Worker* _worker;

private slots:

  void processMessage();
  void emitDataReceived();
  void emitClosed();
  void callShutdown();

signals:
    void trigger_message_processing();
};


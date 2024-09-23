#ifndef ERROR_COLLECTORS_H
#define ERROR_COLLECTORS_H

#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/compiler/importer.h>

#include <QStringList>

#include "absl/strings/string_view.h"

class IoErrorCollector: public google::protobuf::io::ErrorCollector
{
public:
  void RecordError(int line, google::protobuf::io::ColumnNumber column,
                const absl::string_view message) override;

  void RecordWarning(int line, google::protobuf::io::ColumnNumber column,
                  const absl::string_view message) override;

  const QStringList& errors()
  {
    return _errors;
  }

private:
  QStringList _errors;
};

class FileErrorCollector: public google::protobuf::compiler::MultiFileErrorCollector
{
public:
  void RecordError(const absl::string_view filename, int line, int,
                const absl::string_view message) override;

  void RecordWarning(const absl::string_view filename, int line, int,
                  const absl::string_view message) override;

  const QStringList& errors()
  {
    return _errors;
  }

private:
  QStringList _errors;
};

<<<<<<< HEAD
#endif  // ERROR_COLLECTORS_H
=======
#endif // ERROR_COLLECTORS_H
>>>>>>> 93616d01 (Use RecordError and RecordWarning)

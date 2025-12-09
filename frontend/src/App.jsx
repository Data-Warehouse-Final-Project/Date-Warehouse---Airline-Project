import { useState } from 'react';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';

function App() {
  const [file, setFile] = useState(null);
  const [fileMessage, setFileMessage] = useState("");
  const [table, setTable] = useState('facttravelagency');
  const [uploadProgress, setUploadProgress] = useState(0);
  const [isUploading, setIsUploading] = useState(false);
  const [etlLogs, setEtlLogs] = useState(null);
  const [eligibilityMessage, setEligibilityMessage] = useState("");
  const [passengerName, setPassengerName] = useState("");
  const [flightNumber, setFlightNumber] = useState("");
  const [passengerId, setPassengerId] = useState("");

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setFileMessage(""); 
  };


  const handleSubmit = () => {
    if (!file) {
      setFileMessage('Please upload a file first.');
      return;
    }

    // Prepare form data
    const form = new FormData();
    form.append('file', file);
    form.append('table', table);

    // Use XHR to get upload progress events
    const xhr = new XMLHttpRequest();
    const url = import.meta.env.VITE_API_URL || 'http://localhost:3000/process/extract';

    xhr.upload.onprogress = (ev) => {
      if (ev.lengthComputable) {
        const percent = Math.round((ev.loaded / ev.total) * 100);
        setUploadProgress(percent);
      }
    };

    xhr.onload = () => {
      setIsUploading(false);
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const json = JSON.parse(xhr.responseText);
          setFileMessage('Upload started — streaming logs below');
          setEtlLogs(null);
          // if server returned a runId, open EventSource to stream logs
          if (json.runId) {
            startLogStream(json.runId);
          } else if (json.etlLogs) {
            setFileMessage('Upload and ETL finished');
            setEtlLogs(json.etlLogs || json);
          }
        } catch (e) {
          setFileMessage('Upload finished (invalid JSON response)');
        }
      } else {
        // Try to parse server error JSON to show details (stdout/stderr etc.)
        let msg = `Upload failed: ${xhr.status} ${xhr.statusText}`;
        try {
          const errJson = JSON.parse(xhr.responseText || '{}');
          if (errJson.error) msg += ` — ${errJson.error}`;
          if (errJson.details) msg += ` | details: ${JSON.stringify(errJson.details)}`;
          if (errJson.stderr) msg += ` | stderr: ${errJson.stderr}`;
          if (errJson.stdout) msg += ` | stdout: ${errJson.stdout}`;
        } catch (e) {
          // ignore parse errors
        }
        setFileMessage(msg);
      }
      setUploadProgress(0);
    };

    xhr.onerror = () => {
      setIsUploading(false);
      setFileMessage('Upload error');
      setUploadProgress(0);
    };

    setIsUploading(true);
    setFileMessage('Uploading...');
    xhr.open('POST', url);
    xhr.send(form);
  };

  // Log streaming via EventSource (SSE)
  const startLogStream = (runId) => {
    setFileMessage(`Streaming logs for run ${runId}`);
    const streamUrl = (import.meta.env.VITE_API_URL || 'http://localhost:3000') + `/process/stream/${runId}`;
    const es = new EventSource(streamUrl);
    es.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        if (data.initial) {
          setFileMessage(prev => prev + '\n' + data.initial);
        } else if (data.line) {
          setFileMessage(prev => prev + '\n' + data.line);
        }
      } catch (e) {
        setFileMessage(prev => prev + '\n' + ev.data);
      }
    };
    es.onerror = (e) => {
      setFileMessage(prev => prev + '\n[stream closed]');
      es.close();
    };
  };

  const handleCheckEligibility = () => {
    if (!passengerName || !flightNumber || !passengerId) {
      setEligibilityMessage("Please fill in all fields first.");
      return;
    }
    setEligibilityMessage("Passenger is ELIGIBLE for insurance claim");
  };

  return (
    <Container fluid className="py-4">
      <Row className="g-4 justify-content-center align-items-stretch">

        {/* LEFT SIDE: File Upload */}
        <Col xs={12} md={6}>
          <div className="card-section">
            <h4 className="section-title">Upload File</h4>

            <Form.Group controlId="formFile" className="mb-3">
              <h5 className="section-subtitle">Please select file to upload:</h5>
              {/* table is sent programmatically; no dropdown needed */}
              <Form.Control 
                type="file" 
                onChange={handleFileChange} 
                aria-label="Choose file to upload"
              />
            </Form.Group>

            <Button type="button" className="w-100 mb-2" onClick={handleSubmit}>
              Process File (Transform)
            </Button>

            {fileMessage && <p className="message-text">{fileMessage}</p>}
          </div>
        </Col>

        {/* RIGHT SIDE: Search Record */}
        <Col xs={12} md={6}>
          <div className="card-section">
            <h4 className="section-title">Search Record</h4>

            <Form className="flex-grow-1 d-flex flex-column justify-content-center w-100">
              <h5 className="section-subtitle">Please input record to search:</h5>

              <Form.Group as={Row} className="mb-3" controlId="nameInput">
                <Form.Label column sm="4" className="fw-bold">Name:</Form.Label>
                <Col sm="8">
                  <Form.Control
                    type="text"
                    placeholder="Enter name"
                    value={passengerName}
                    onChange={(e) => setPassengerName(e.target.value)}
                  />
                </Col>
              </Form.Group>

              <Form.Group as={Row} className="mb-3" controlId="flightInput">
                <Form.Label column sm="4" className="fw-bold">Flight Number:</Form.Label>
                <Col sm="8">
                  <Form.Control
                    type="text"
                    placeholder="Enter flight number"
                    value={flightNumber}
                    onChange={(e) => setFlightNumber(e.target.value)}
                  />
                </Col>
              </Form.Group>

              <Form.Group as={Row} className="mb-3" controlId="passengerIdInput">
                <Form.Label column sm="4" className="fw-bold">Passenger ID:</Form.Label>
                <Col sm="8">
                  <Form.Control
                    type="text"
                    placeholder="Enter passenger ID"
                    value={passengerId}
                    onChange={(e) => setPassengerId(e.target.value)}
                  />
                </Col>
              </Form.Group>
            </Form>

            <div className="mt-3">
              <Button type="button" className="w-100 mb-2" onClick={handleCheckEligibility}>
                Check Eligibility
              </Button>

              {eligibilityMessage && <p className="message-text">{eligibilityMessage}</p>}
            </div>
          </div>
        </Col>

      </Row>
    </Container>
  );
}

export default App;

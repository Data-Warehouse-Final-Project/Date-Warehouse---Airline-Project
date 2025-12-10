import { useState } from 'react';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';
//import { supabase } from './components/supabaseClient';

function App() {
  const [file, setFile] = useState(null);
  const [fileMessage, setFileMessage] = useState("");
  const [fileMessageType, setFileMessageType] = useState("");
  const [eligibilityMessage, setEligibilityMessage] = useState("");
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [flightNumber, setFlightNumber] = useState("");
  const [passengerId, setPassengerId] = useState("");
  const [fileType, setFileType] = useState("");
  const [cleanedTables, setCleanedTables] = useState(false);

  const fileTypes = [
    "airlines",
    "airports",
    "flights",
    "passengers",
    "transactions"
  ];

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (!selectedFile) return;

    // =========================================
    // ENSURE ONLY CSV FILES ARE UPLOADED
    // =========================================
    const validTypes = ["text/csv", "application/vnd.ms-excel"];
    const fileName = selectedFile.name.toLowerCase();
    if (!validTypes.includes(selectedFile.type) && !fileName.endsWith(".csv")) {
      setFile(null);
      setFileMessage("Only CSV files are allowed!");
      setFileMessageType("error");
      setCleanedTables(false);
      return;
    }

    setFile(selectedFile);
    setFileMessage("");
    setFileMessageType("");
    setCleanedTables(false);
  };

  // ============================================================
  // CLEAN TABLES
  // ============================================================
  const handleClean = async () => {
    if (!file) {
      setFileMessage("Please upload a file first.");
      setFileMessageType("error");
      return;
    }
    if (!fileType) {
      setFileMessage("Please select the CSV type before cleaning.");
      setFileMessageType("error");
      return;
    }

    setFileMessage("Cleaning in progress...");
    setFileMessageType("info");

    try {
      const backendUrl = import.meta.env.VITE_BACKEND_URL || 'http://localhost:3001';
      const formData = new FormData();
      formData.append("file", file);
      formData.append("file_type", fileType);

      const resp = await fetch(`${backendUrl}/api/clean-file`, {
        method: "POST",
        body: formData
      });

      const result = await resp.json();
      if (resp.ok) {
        setFileMessage(`${fileType.charAt(0).toUpperCase() + fileType.slice(1)} file cleaned successfully. Quarantined CSV saved.`);
        setFileMessageType("success");
        setCleanedTables(true);
      } else {
        setFileMessage(`Error during cleaning: ${result.error || "Unknown error"}`);
        setFileMessageType("error");
      }
    } catch (err) {
      setFileMessage(`Error contacting backend for cleaning: ${err.message}`);
      setFileMessageType("error");
    }
  };

  // ============================================================
  // TRANSFORM TABLES
  // ============================================================
  const handleSubmit = async () => {
    if (!file) {
      setFileMessage("Please upload a file first.");
      setFileMessageType("error");
      return;
    }
    if (!fileType) {
      setFileMessage("Please select the CSV type before transforming.");
      setFileMessageType("error");
      return;
    }
    if (!cleanedTables) {
      setFileMessage("Please clean the file before transforming.");
      setFileMessageType("error");
      return;
    }

    setFileMessage("Transforming tables and upserting to database...");
    setFileMessageType("info");

    try {
      const backendUrl = import.meta.env.VITE_BACKEND_URL || 'http://localhost:3001';
      const resp = await fetch(`${backendUrl}/api/transform-tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_type: fileType })
      });

      const result = await resp.json();
      if (resp.ok) {
        setFileMessage(`${fileType.charAt(0).toUpperCase() + fileType.slice(1)} tables transformed and upserted successfully.`);
        setFileMessageType("success");
      } else {
        setFileMessage(`Error during transform: ${result.error || "Unknown error"}`);
        setFileMessageType("error");
      }
    } catch (err) {
      setFileMessage(`Error contacting backend for transform: ${err.message}`);
      setFileMessageType("error");
    }
  };

  const handleCheckEligibility = () => {
    // Perform an API call to backend which will produce a Kafka message
    // and check Supabase for the flight times to determine eligibility.
    const performCheck = async () => {
      if (!firstName || !lastName || !flightNumber || !passengerId) {
        setEligibilityMessage("Please fill in all fields first.");
        return;
      }
      setEligibilityMessage('Queued — waiting for eligibility result...');
      try {
        const backendUrl = import.meta.env.VITE_BACKEND_URL || 'http://localhost:3001';
        const resp = await fetch(`${backendUrl}/api/check-eligibility`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ firstName, lastName, flightNumber, passengerId })
        });

        const requestTimeoutMs = 30000; // stop listening after 30s
        let timedOut = false;

        const channel = supabase
          .channel(`eligibility-${passengerId}-${flightNumber}-${Date.now()}`)
          .on(
            'postgres_changes',
            {
              event: 'INSERT',
              schema: 'public',
              table: 'eligibility_results',
              filter: `passenger_id=eq.${passengerId}`
            },
            (payload) => {
              if (timedOut) return;
              const row = payload.new;
              if (!row) return;
              const { eligible, delay_minutes, reason } = row;
              if (eligible) {
                setEligibilityMessage(`Passenger is ELIGIBLE for insurance claim (delay ${delay_minutes} minutes)`);
              } else {
                if (reason === 'flight_not_found') setEligibilityMessage('No flight record found for this flight number.');
                else if (reason === 'missing_time_data') setEligibilityMessage('Flight time data missing; cannot determine eligibility.');
                else setEligibilityMessage(`Passenger is NOT eligible (delay ${delay_minutes ?? 'N/A'} minutes)`);
              }

              channel.unsubscribe();
            }
          )
          .subscribe();

        const to = setTimeout(() => {
          timedOut = true;
          try { channel.unsubscribe(); } catch (e) {}
          setEligibilityMessage('Timed out waiting for eligibility result. Try again or check logs.');
        }, requestTimeoutMs);

      } catch (err) {
        setEligibilityMessage('Error contacting backend for eligibility check.');
      }
    };
    performCheck();
  };

  return (
    <Container fluid className="py-4">
      <Row className="g-4 justify-content-center align-items-stretch">

        {/* LEFT SIDE: File Upload */}
        <Col xs={12} md={6}>
          <div className="card-section upload-section">
            <h4 className="section-title upload-title">Upload File</h4>

            <Form.Group controlId="formFile" className="mb-3 file-upload-group">
              <h5 className="section-subtitle">Please select file to upload:</h5>
              <Form.Control 
                type="file" 
                onChange={handleFileChange} 
                aria-label="Choose file to upload"
              />
            </Form.Group>

            <div className="mb-3 csv-type-selector">
              <h5 className="section-subtitle">Select CSV type:</h5>
              <div className="d-flex flex-wrap gap-2 csv-type-buttons">
                {fileTypes.map((type) => {
                  const isActive = fileType === type;
                  return (
                    <Button
                      key={type}
                      type="button"
                      className="text-capitalize"
                      style={fileType === type ? { backgroundColor: '#1c2b61', color: 'white', border: '1px solid #1c2b61' } : { backgroundColor: 'white', color: '#1c2b61', border: '1px solid #1c2b61' }}
                      onClick={() => {
                        setFileType(type);
                        setFileMessage("");
                        setFileMessageType("");
                      }}
                      disabled={!file}
                    >
                      {type}
                    </Button>
                  );
                })}
              </div>
            </div>

            <Button type="button" className="w-100 mb-2 custom-btn clean-btn" onClick={handleClean} disabled={!file || !fileType}>
              Clean Tables
            </Button>

            <Button type="button" className="w-100 mb-2 custom-btn transform-btn" onClick={handleSubmit} disabled={!file || !fileType || !cleanedTables}>
              Transform Tables
            </Button>
          </div>
          <div className="file-message-container">
            {fileMessage && (
              <p className={`message-text ${fileMessageType}`}>
                {fileMessage}
              </p>
            )}
          </div>
        </Col>

        {/* RIGHT SIDE: Search Record */}
        <Col xs={12} md={6}>
          <div className="card-section search-section">
            <h4 className="section-title search-title">Search Record</h4>

            <Form className="flex-grow-1 d-flex flex-column justify-content-center w-100 search-form">
              <h5 className="section-subtitle">Please input record to search:</h5>

              <Form.Group as={Row} className="mb-3 search-name-group" controlId="firstNameInput">
                <Form.Label column sm="4">First Name:</Form.Label>
                <Col sm="8">
                  <Form.Control
                    type="text"
                    placeholder="Enter first name"
                    value={firstName}
                    onChange={(e) => setFirstName(e.target.value)}
                  />
                </Col>
              </Form.Group>

              <Form.Group as={Row} className="mb-3 search-name-group" controlId="lastNameInput">
                <Form.Label column sm="4">Last Name:</Form.Label>
                <Col sm="8">
                  <Form.Control
                    type="text"
                    placeholder="Enter last name"
                    value={lastName}
                    onChange={(e) => setLastName(e.target.value)}
                  />
                </Col>
              </Form.Group>

              <Form.Group as={Row} className="mb-3 search-flight-group" controlId="flightInput">
                <Form.Label column sm="4">Flight Number:</Form.Label>
                <Col sm="8">
                  <Form.Control
                    type="text"
                    placeholder="Enter flight number"
                    value={flightNumber}
                    onChange={(e) => setFlightNumber(e.target.value)}
                  />
                </Col>
              </Form.Group>

              <Form.Group as={Row} className="mb-3 search-passenger-group" controlId="passengerIdInput">
                <Form.Label column sm="4">Passenger ID:</Form.Label>
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

            <div className="mt-3 eligibility-button-section">
              <Button type="button" className="w-100 mb-2 custom-btn eligibility-btn" onClick={handleCheckEligibility} disabled={!firstName || !lastName || !flightNumber || !passengerId}>
                Check Eligibility
              </Button>
            </div>
          </div>
          <div className="eligibility-message-container">
            {eligibilityMessage && <p className="message-text">{eligibilityMessage}</p>}
          </div>
        </Col>

      </Row>
    </Container>
  );
}

export default App;
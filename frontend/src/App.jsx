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
      setFileMessage("Please upload a file first.");
      return;
    }
    setFileMessage("File processed successfully");
    setEligibilityMessage("");
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

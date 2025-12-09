

const { supabase } = require('../config/supabaseClient'); // Import the Supabase client

// Controller to check eligibility based on passenger ID and flight number
exports.checkEligibility = async (req, res) => {
  const { passengerID, flightNumber } = req.body; // Destructure the fields from the request body

  try {
    // Query the Fact_BookingSales table to check eligibility
    const { data, error } = await supabase
      .from('Fact_BookingSales')  // Use the appropriate table name
      .select('PassengerSK, FlightSK, TicketPrice, Taxes, BaggageFees, TotalAmount')
      .eq('PassengerID', passengerID)  // Match the passenger ID
      .eq('FlightNumber', flightNumber)  // Match the flight number
      .single();  // Return a single result (if any)

    // Check for errors or no data found
    if (error || !data) {
      return res.status(404).json({ message: 'No eligibility found for this passenger and flight' });
    }

    // Return the eligibility details
    return res.json({ eligible: true, details: data });
  } catch (err) {
    // Handle any other errors
    console.error('Error checking eligibility:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};
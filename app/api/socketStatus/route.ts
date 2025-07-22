import { NextResponse } from 'next/server';
import { io } from "socket.io-client";

// export const config = {
//   api: {
//     externalResolver: true,
//   },
// };

export async function GET() {
  let clientSocket = io('http://localhost:5001'); // Change this to your Flask server URL

  clientSocket.on("connect", () => {
    console.log(clientSocket.id); // log the socket ID to the console.
    console.log("Connected");
  });

  // log all incoming socket data to a variable of 'data' from the socket topic of 'execution_result`
  clientSocket.on("execution_result", data => {
    console.log(data);
  });

  return NextResponse.json({ message: 'Socket initialized' });
}

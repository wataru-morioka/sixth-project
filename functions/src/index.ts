import * as functions from 'firebase-functions';

// // Start writing Firebase Functions
// // https://firebase.google.com/docs/functions/typescript
//
// export const helloWorld = functions.https.onRequest((request, response) => {
//  response.send("Hello from Firebase!");
// });

exports.hello = functions.https.onRequest((request, response) => {
    response.status(200).send("Hello World")
})

exports.createQuestion = functions.firestore
    .document('questions/{questionId}')
    .onCreate((snap, context) => {
      // Get an object representing the document
      // e.g. {'name': 'Marie', 'age': 66}
        const newValue = snap.data();

      // access a particular field as you would any JS property
        // const name = newValue.name;
        if (newValue != undefined) {
            console.log(newValue.uid)
        }
        console.log(context.params.questionId)
      // perform desired operations ...
});

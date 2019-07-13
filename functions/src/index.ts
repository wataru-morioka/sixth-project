import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import 'moment/locale/ja'
import * as moment from 'moment';
// moment.locale('ja');
admin.initializeApp(functions.config().firebase);
const db = admin.firestore();

exports.extractTargets = functions.region('asia-northeast1').firestore
  .document('questions/{questionId}')
  .onCreate(async (snap, context) => {
    const question = snap.data();
    if (question === undefined) {
      return;
    }
    
    const users = db.collection('users');
    let count: number = 0;
    let targetNumber: number = 0;

    await users.get()
      .then(allUsers => {
        count = allUsers.size;
      })
      .catch(err => {
          console.log('Error getting documents', err);
      });

    if (count < question.targetNumber) {
      targetNumber = count;
    } else {
      targetNumber = question.targetNumber;
    }

    const targetArray: string[] = new Array();

    while (targetArray.length < targetNumber) {
      console.log(targetArray.length)
      const key = users.doc().id;
      let exist = false;

      await users.where(admin.firestore.FieldPath.documentId(), '>=', key)
          .limit(1)
          .get()
          .then(async targets => {
              if(targets.size > 0) {
                exist = true;
                Promise.all(targets.docs.map(async target => {
                    if (targetArray.indexOf(target.id) >= 0) {
                      return;
                    }
                    console.log(target.id, '=>', target.data());
                    targetArray.push(target.id);
                    addTargets(target.id, context.params.questionId, question.timePeriod);
                  })
                ).then( _ => {
                    console.log("登録完了");
                  }
                ).catch(err => {
                    console.log("登録エラー");
                  }
                );
              }
          })
          .catch(err => {
              console.log('Error getting documents', err);
          });
      
      if (exist) {
        continue;
      }

      await users.where(admin.firestore.FieldPath.documentId(), '<=', key)
          .limit(1)
          .get()
          .then(async targets => {
            Promise.all(targets.docs.map(async target => {
                if (targetArray.indexOf(target.id) >= 0) {
                  return;
                }
                console.log(target.id, '=>', target.data());
                targetArray.push(target.id);
                addTargets(target.id, context.params.questionId, question.timePeriod);
              })
            ).then( _ => {
                console.log("登録完了");
              }
            ).catch(err => {
                console.log("登録エラー");
              }
            );
          })
          .catch(err => {
              console.log('Error getting documents', err);
          });
    }
    
    return 0;
});

function addTargets(uid: string, questionId: string, timePeriod: number) {

  const now = moment().add(9, 'hour').format("YYYY-MM-DD HH:mm:ss");
  const timeLimit = moment().add(9, 'hour').add(timePeriod, "minute").format("YYYY-MM-DD HH:mm:ss");
  db.collection('targets').add({
    "uid": uid,
    "serverQuestionId": questionId,
    "timeLimit": timeLimit,
    "askPushFlag": false,
    "askReceiveFlag": false,
    "answerFlag": false,
    "determinationFlag": false,
    "finalPushFlag": false,
    "resultReceiveFlag": false,
    "createdDateTime": now,
    "modifiedDateTime": null,
  }).then(function(targetDoc) {
    console.log("Document written with ID: ", targetDoc.id);
  }).catch(function(error) {
    console.error("Error adding document: ", error);
  });
}

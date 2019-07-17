import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import * as moment from 'moment';
import { WriteBatch } from '@google-cloud/firestore';
admin.initializeApp(functions.config().firebase);
const db = admin.firestore();
const options = {
  priority: "high",
};

exports.extractTargets = functions.region('asia-northeast1').firestore
  .document('questions/{questionId}')
  .onCreate(async (snap, context) => {
    const question = snap.data();
    if (question === undefined) {
      return 1;
    }
    console.log('新規質問受信：' + question.id);
    const users = db.collection('users');
    let count: number = 0;
    let targetNumber: number = 0;

    await users.get().then(allUsers => {
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
      const key = users.doc().id;
      let exist = false;

      await users
      .where(admin.firestore.FieldPath.documentId(), '>=', key)
      .limit(1)
      .get()
      .then(userDocs => {
        if(userDocs.size <= 0) { return; }
        exist = true;
        Promise.all(userDocs.docs.map(user => {
          if (targetArray.indexOf(user.id) >= 0 || user.id === question.uid) { return; }
          console.log(user.id, '=>', user.data());
          targetArray.push(user.id);
          addTargets(user.id, context.params.questionId, question.minutes);
        })
        ).then( _ => {
          console.log('登録完了');
        })
        .catch(err => {
          console.log('登録エラー', err);
        });
      })
      .catch(err => {
        console.log('Error getting documents', err);
      });
      
      if (exist) { continue; }

      await users
      .where(admin.firestore.FieldPath.documentId(), '<=', key)
      .limit(1)
      .get()
      .then(userDocs => {
        Promise.all(userDocs.docs.map(user => {
          if (targetArray.indexOf(user.id) >= 0 || user.id === question.uid) { return; }
          console.log(user.id, '=>', user.data());
          targetArray.push(user.id);
          addTargets(user.id, context.params.questionId, question.minutes);
        })
        ).then( _ => {
          console.log('登録完了');
        })
        .catch(err => {
          console.log('登録エラー');
        });
      })
      .catch(err => {
        console.log('Error getting documents', err);
      });
    }
    return 0;
});

exports.aggregate = functions.region('asia-northeast1').https.onRequest( async (request, response) => {
  setTimeout( () => {
    response.send('集計処理バッチ開始');
  }, 1)
  // let loopFlag = true;
  // while (loopFlag) {
    console.log('ループ');
    let result = true;
    //集計対象の質問を抽出
    const now = moment().add(9, 'hour').format('YYYY-MM-DD HH:mm:ss');
    const targetQuestionIdArray: string[] = new Array();
    const questions = db.collection('questions');
    await questions
    .where('determinationFlag', '==', false)
    .where('timeLimit', '<', now)
    .get()
    .then(async targetQuestions => {
      Promise.all(
        targetQuestions.docs.map(targetQuestion => {
          console.log('抽出対象：' + targetQuestion.id);
          targetQuestionIdArray.push(targetQuestion.id);
        })
      ).then( _ => {
          console.log('集計対象抽出成功');
        }
      ).catch(err => {
          console.log('集計対象抽出エラー');
          result = false;
        }
      );
    })
    .catch(err => {
      console.log('Error getting documents', err);
      result = false;
    });
    console.log('抽出完了');

    //非同期で各質問の集計をし、（questions,targets,answers）コレクションを更新※トランザクション
    Promise.all(
      targetQuestionIdArray.map(async questionId => {
        //集計
        let answer1number = 0;
        let answer2number = 0;

        await Promise.all([
          aggregate(questionId, 1),
          aggregate(questionId, 2)
          ]
        ).then(results => {
          answer1number = results[0];
          answer2number = results[1];
        }).catch(err => {
          console.log(err);
          result = false;
        });

        console.log('トランザクション開始');
        const batch = db.batch();
        await Promise.all([
          updateTargets(batch, questionId),
          updateAnswers(batch, questionId),
          updateQuestion(batch, questionId,answer1number,answer2number)
        ])
        .then(async results => {
          console.log('コミット開始', results);
          await batch.commit()
          .then(function () {
            console.log("トランザクション完了");
          })
          .catch(err => {
            console.log('コミットエラー', err);
            result = false;
          }); 
        })
        .catch(err => {
          console.log('集計処理エラー', err);
          result = false;
        })
     
        if (!result) {
          // loopFlag = result;
          return;
        }
      })
    ).then( _ => {
      console.log('集計バッチ完了');
    })
    .catch(err => {
      console.log('集計バッチエラー');
      // loopFlag = false;
    });

  //   await sleep(60);
  // }
});

const aggregate = (questionId: string, decision: number) => {
  return new Promise<number>(async (resolve, reject) => {
    await db.collection('answers')
    .where('serverQuestionId', '==', questionId)
    .where('decision', '==', decision)
    .get()
    .then(results => {
      console.log(decision + ' questionId:' + questionId + ' ' + results.size);
      resolve(results.size);
      return;
    })
    .catch(err => {
      reject(-1);
      return;
    });
  }); 
}

const updateTargets = (batch: WriteBatch, questionId: string) => {
  return new Promise<boolean>(async (resolve, reject) => {
    console.log('targets更新開始');
    await db.collection('targets')
    .where('serverQuestionId', '==', questionId)
    .get()
    .then(targets => {
      Promise.all(
        targets.docs.map(async target => {
          console.log(target.id);
          const targetRef = db.collection('targets').doc(target.id); 
          batch.update(targetRef, {
            'determinationFlag': true 
          });
        })
      ).then( async _ => {
          console.log('targets更新完了');
          resolve(true);
        }
      )
      .catch(err => {
          console.log('targets更新エラー');
          reject(false);
        }
      );
    })
    .catch(err => {
      console.log('Error2 getting documents', err);
      reject(false);
    });
  })
}

const updateAnswers = (batch: WriteBatch, questionId: string) => {
  return new Promise<boolean>(async (resolve, reject) => {
    console.log('answers更新開始');
    await db.collection('answers')
    .where('serverQuestionId', '==', questionId)
    .get()
    .then(answers =>{
      Promise.all(
        answers.docs.map(async answer => {
          console.log(answer.id);
          const answerRef = db.collection('answers').doc(answer.id);
          batch.update(answerRef, {
            'determinationFlag': true
          });
        })
      ).then( test => {
          console.log('answers更新完了');
          resolve(true);
        }
      ).catch(err => {
          console.log('answers更新エラー');
          reject(false);
        }
      );
    })
    .catch(err => {
      console.log('Error3 getting documents', err);
      reject(false);
    });
  })
}

const updateQuestion = (batch: WriteBatch, questionId: string, answer1number: number, answer2number: number) => {
  return new Promise<boolean>((resolve, reject) => {
    console.log('questions更新開始');
    const questionRef = db.collection('questions').doc(questionId);
    batch.update(questionRef, {
      'answer1number': answer1number,
      'answer2number': answer2number,
      'determinationFlag': true
    });
    resolve(true);
  })
}

exports.pushAskingToTargets = functions.region('asia-northeast1').firestore
  .document('targets/{targetId}')
  .onCreate(async (snap, context) => {
    const target = snap.data();
    if (target === undefined) {
      return 1;
    }

    //プッシュ通知
    const payload = {
      notification: {
        title: '新着質問',
        body: '新しい質問を受信しました',
        badge: "1",
        sound:"default",
      }
    };

    await notify(payload, target['uid']);

    await db.collection('targets').doc(context.params.targetId).update({
      'askPushFlag': true
    });
    
    return 0;
});

exports.pushResultTargets = functions.region('asia-northeast1').firestore
  .document('targets/{targetId}')
  .onUpdate( async (snap, context) => {
    const target = snap.after.data();
    if (target === undefined) {
      return 1;
    }

    if (!target['determinationFlag'] || target['resultReceiveFlag'] || target['finalPushFlag']) {
      return 0;
    }

    //プッシュ通知
    const payload = {
      notification: {
        title: '集計結果受信',
        body: '他人の質問の集計が完了しました',
        badge: "1",
        sound:"default",
      },
    };

    await notify(payload, target['uid']);

    await db.collection('targets').doc(context.params.targetId).update({
      'finalPushFlag': true
    });
    
    return 0;
});

exports.pushResultToOwners = functions.region('asia-northeast1').firestore
  .document('questions/{questionId}')
  .onUpdate( async (snap, context) => {
    const question = snap.after.data();
    if (question === undefined) {
      return 1;
    }

    if (!question['determinationFlag'] || question['resultReceiveFlag'] || question['finalPushFlag']) {
      return 0;
    }

    //プッシュ通知
    const payload = {
      notification: {
        title: '集計結果受信',
        body: '自分の質問の集計が完了しました',
        badge: "1",
        sound:"default",
      }
    };

    await notify(payload, question['uid']);

    await db.collection('questions').doc(context.params.questionId).update({
      'finalPushFlag': true
    });
    
    return 0;
});

exports.deleteTargets = functions.region('asia-northeast1').firestore
  .document('targets/{targetId}')
  .onUpdate( async (snap, context) => {
    const target = snap.after.data();
    if (target === undefined) {
      return 1;
    }

    if (target['resultReceiveFlag'] && target['finalPushFlag']) {
      console.log('削除target:'+ context.params.targetId);
      await db.collection('targets').doc(context.params.targetId).delete();
    }
    
    return 0;
});

exports.deleteAnswers = functions.region('asia-northeast1').firestore
  .document('answers/{answerId}')
  .onUpdate( async (snap, context) => {
    const answer = snap.after.data();
    if (answer === undefined) {
      return 1;
    }

    if (answer['determinationFlag']) {
      console.log('削除target:'+ context.params.answerId);  
      await db.collection('answers').doc(context.params.answerId).delete();
    }
    
    return 0;
});

// const sleep = (waitSeconds: number) => {
//   return new Promise(resolve => {
//       setTimeout(() => {
//         resolve();
//     }, waitSeconds * 1000);
//   })
// }

function addTargets(uid: string, questionId: string, minutes: number) {
  const now = moment().add(9, 'hour').format('YYYY-MM-DD HH:mm:ss');
  const timeLimit = moment().add(9, 'hour').add(minutes, 'minute').format('YYYY-MM-DD HH:mm:ss');

  const targetRef = admin.firestore().collection('targets').doc(`${uid}_${questionId}`);
  const questionRef = db.collection('questions').doc(questionId);
  
  const batch = db.batch();

  batch.set(targetRef, {
    'uid': uid,
    'serverQuestionId': questionId,
    'timeLimit': timeLimit,
    'askPushFlag': false,
    'askReceiveFlag': false,
    'answerFlag': false,
    'determinationFlag': false,
    'finalPushFlag': false,
    'resultReceiveFlag': false,
    'createdDateTime': now,
    'modifiedDateTime': null,
  });

  batch.update(questionRef, {
    'timeLimit': timeLimit,
    'askFlag': true
  });

  batch.commit()
  .then(function () {
    console.log("トランザクション完了");
  })
  .catch(err => {
    console.log('Error getting documents', err);
  });
}

async function notify(payload: {}, uid: string) {
  let token: string = ''
  let result = false;
  await db.collection('users').doc(uid).get().then(user => {
    const userInfo = user.data()
    if (userInfo === undefined) {
      return;
    }
    token = userInfo.token;
    if (token.length > 0) {
      console.log(token);
      result = true;
    }
  })

  if (!result) {
    return;
  }

  admin.messaging().sendToDevice(token, payload, options)
  .then(pushResponse => {
    console.log("Successfully sent message:", pushResponse);
  })
  .catch(error => {
    console.log("Error sending message:", error);
  });
}